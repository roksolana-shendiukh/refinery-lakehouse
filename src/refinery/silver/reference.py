"""Silver reference tables: keep the latest CSV version, type the columns, define the DQX checks."""

import databricks.labs.dqx.geo.check_funcs  # noqa: F401  registers the geometry checks used from metadata
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from refinery.silver.quality import build_check, exists_in, expression, not_null, one_of, unique


def latest_version(df: DataFrame) -> DataFrame:
    """Rows of the newest landed version of a reference CSV: newest load, then newest file name."""
    newest = df.agg(F.max(F.struct("ingested_at", "source_file")).alias("newest")).select("newest.source_file")
    return df.join(F.broadcast(newest), "source_file")


# How the STM cleans each reference column: trimmed text, blank becomes NULL, keys upper-cased.
NORMALIZE = {
    "key": lambda c: F.when(F.trim(c) != "", F.upper(F.trim(c))),
    "text": lambda c: F.when(F.trim(c) != "", F.trim(c)),
    "lower": lambda c: F.lower(F.trim(c)),
    "upper": lambda c: F.upper(F.trim(c)),
    "boolean": lambda c: F.lower(F.trim(c)).try_cast("boolean"),
    "trim": lambda c: F.trim(c),
}

COLUMN_KINDS = {
    "ref_area": {
        "area_bk": "key",
        "area_name": "text",
        "area_level": "lower",
        "parent_area_bk": "key",
        "is_coastal": "boolean",
    },
    "ref_hub": {"hub_bk": "key", "hub_name": "text", "served_area_bk": "key"},
    "ref_product": {
        "product_bk": "key",
        "product_name": "text",
        "product_family": "text",
        "unit": "upper",
        "representative_grade_bk": "key",
    },
    "ref_fuel_grade": {"grade_bk": "key", "grade_name": "text", "source_unit": "upper", "product_bk": "key"},
    "ref_eia_product": {"eia_product": "key", "price_type": "lower", "grade_bk": "key"},
    "ref_terminal_zone": {
        "port_bk": "key",
        "port_name": "text",
        "terminal_type": "lower",
        "area_bk": "key",
        "boundary_wkt": "trim",
    },
    "ref_mid": {"mid": "text", "country_name": "text"},
}


def to_silver(df: DataFrame, name: str) -> DataFrame:
    """Latest version of a bronze reference table with the silver columns, cleaned as the STM says.

    A value that cannot be parsed becomes NULL, so a check can reject the row.
    """
    columns = [NORMALIZE[kind](F.col(column)).alias(column) for column, kind in COLUMN_KINDS[name].items()]
    return latest_version(df).select(*columns, F.col("ingested_at").alias("loaded_at"), "source_file")


def _metadata_checks(table):
    """Checks every reference table has: when it was loaded and from which file."""
    return [not_null(table, "loaded_at", "error"), not_null(table, "source_file", "warn")]


CHECKS = {
    "ref_area": [
        not_null("ref_area", "area_bk", "error"),
        unique("ref_area", ["area_bk"], "error"),
        not_null("ref_area", "area_name", "warn"),
        not_null("ref_area", "area_level", "error"),
        one_of("ref_area", "area_level", ["country", "region", "subregion"], "error"),
        expression(
            "ref_area",
            "parent_area_bk",
            "null_only_for_country",
            "error",
            "(area_level = 'country') = (parent_area_bk IS NULL)",
            "parent_area_bk must be NULL for a country and set for any other level",
        ),
        exists_in("ref_area", "parent_area_bk", "ref_area", "area_bk", "error"),
        not_null("ref_area", "is_coastal", "warn"),
        *_metadata_checks("ref_area"),
    ],
    "ref_hub": [
        not_null("ref_hub", "hub_bk", "error"),
        unique("ref_hub", ["hub_bk"], "error"),
        not_null("ref_hub", "hub_name", "warn"),
        exists_in("ref_hub", "served_area_bk", "ref_area", "area_bk", "warn"),
        *_metadata_checks("ref_hub"),
    ],
    "ref_product": [
        not_null("ref_product", "product_bk", "error"),
        unique("ref_product", ["product_bk"], "error"),
        not_null("ref_product", "product_name", "warn"),
        not_null("ref_product", "product_family", "warn"),
        one_of("ref_product", "unit", ["MBBL/D"], "error"),
        not_null("ref_product", "unit", "error"),
        *_metadata_checks("ref_product"),
    ],
    "ref_fuel_grade": [
        not_null("ref_fuel_grade", "grade_bk", "error"),
        unique("ref_fuel_grade", ["grade_bk"], "error"),
        not_null("ref_fuel_grade", "grade_name", "warn"),
        one_of("ref_fuel_grade", "source_unit", ["$/GAL", "$/BBL"], "warn"),
        not_null("ref_fuel_grade", "source_unit", "warn"),
        expression(
            "ref_fuel_grade",
            "product_bk",
            "null_only_for_barrels",
            "warn",
            "(source_unit = '$/BBL') = (product_bk IS NULL)",
            "product_bk must be NULL for a $/BBL grade and set for any other unit",
        ),
        *_metadata_checks("ref_fuel_grade"),
    ],
    "ref_eia_product": [
        not_null("ref_eia_product", "eia_product", "error"),
        not_null("ref_eia_product", "price_type", "error"),
        one_of("ref_eia_product", "price_type", ["retail", "spot"], "error"),
        not_null("ref_eia_product", "grade_bk", "error"),
        exists_in("ref_eia_product", "grade_bk", "ref_fuel_grade", "grade_bk", "error"),
        unique("ref_eia_product", ["eia_product", "price_type"], "error"),
        unique("ref_eia_product", ["grade_bk", "price_type"], "error"),
        *_metadata_checks("ref_eia_product"),
    ],
    "ref_terminal_zone": [
        not_null("ref_terminal_zone", "port_bk", "error"),
        unique("ref_terminal_zone", ["port_bk"], "error"),
        not_null("ref_terminal_zone", "port_name", "warn"),
        one_of(
            "ref_terminal_zone",
            "terminal_type",
            ["refinery dock", "product terminal", "crude terminal", "anchorage"],
            "warn",
        ),
        not_null("ref_terminal_zone", "terminal_type", "warn"),
        not_null("ref_terminal_zone", "area_bk", "error"),
        # Looks up the coastal areas only: the area must exist in ref_area and have is_coastal = true.
        exists_in("ref_terminal_zone", "area_bk", "ref_area_coastal", "area_bk", "warn", rule="coastal_area"),
        # boundary_wkt (valid geometry) needs Databricks-only functions; it is added with the pipeline.
        *_metadata_checks("ref_terminal_zone"),
    ],
    "ref_mid": [
        not_null("ref_mid", "mid", "error"),
        build_check(
            "ref_mid", "mid", "three_digits_2_to_7", "error", "regex_match", column="mid", regex=r"^[2-7][0-9]{2}$"
        ),
        unique("ref_mid", ["mid"], "error"),
        not_null("ref_mid", "country_name", "warn"),
        *_metadata_checks("ref_mid"),
    ],
}

# Needs Databricks (try_to_geometry, st_isvalid), so it is not in CHECKS and does not run on local Spark.
BOUNDARY_CHECK = build_check(
    "ref_terminal_zone", "boundary_wkt", "valid_geometry", "error", "is_ogc_valid", column="boundary_wkt"
)


def pipeline_checks(name: str) -> list[dict]:
    """The checks a pipeline applies: CHECKS plus the ones that need Databricks."""
    return CHECKS[name] + ([BOUNDARY_CHECK] if name == "ref_terminal_zone" else [])


def lookup_tables(ref_area: DataFrame) -> dict[str, DataFrame]:
    """Area lookups for the foreign key checks: all areas, and the coastal ones."""
    return {"ref_area": ref_area, "ref_area_coastal": ref_area.filter(F.col("is_coastal"))}


def integrity_findings(products: DataFrame, grades: DataFrame) -> DataFrame:
    """Cross-checks between ref_product and ref_fuel_grade, one row per problem (both are warnings).

    They run here and not inside the two tables, because the tables point at each other.
    """
    wrong_grade = (
        products.filter(F.col("representative_grade_bk").isNotNull())
        .join(
            grades.select("grade_bk", F.col("product_bk").alias("grade_product_bk")),
            F.col("representative_grade_bk") == F.col("grade_bk"),
            "left",
        )
        .filter(F.col("grade_bk").isNull() | ~F.col("grade_product_bk").eqNullSafe(F.col("product_bk")))
        .select(
            F.lit("ref_product.representative_grade_bk.exists_and_belongs_to_product").alias("check_name"),
            F.lit("ref_product").alias("table_name"),
            F.col("product_bk").alias("key_value"),
            F.col("representative_grade_bk").alias("bad_value"),
            F.lit("warning").alias("level"),
            "loaded_at",
        )
    )
    unknown_product = (
        grades.filter(F.col("product_bk").isNotNull())
        .join(
            products.select(F.col("product_bk").alias("known_product_bk")),
            F.col("product_bk") == F.col("known_product_bk"),
            "left",
        )
        .filter(F.col("known_product_bk").isNull())
        .select(
            F.lit("ref_fuel_grade.product_bk.exists").alias("check_name"),
            F.lit("ref_fuel_grade").alias("table_name"),
            F.col("grade_bk").alias("key_value"),
            F.col("product_bk").alias("bad_value"),
            F.lit("warning").alias("level"),
            "loaded_at",
        )
    )
    return wrong_grade.unionByName(unknown_product)
