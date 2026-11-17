from datetime import datetime
from unittest.mock import MagicMock

import pytest
from databricks.labs.dqx.engine import DQEngine
from pyspark.sql import functions as F

from refinery.bronze.reference import REFERENCE_COLUMNS
from refinery.silver.quality import check, quarantine
from refinery.silver.reference import (
    CHECKS,
    COLUMN_KINDS,
    integrity_findings,
    latest_version,
    lookup_tables,
    pipeline_checks,
    to_silver,
)

AREA_SCHEMA = "area_bk STRING, area_name STRING, area_level STRING, parent_area_bk STRING, is_coastal BOOLEAN"


@pytest.fixture(scope="module")
def engine(spark):
    return DQEngine(MagicMock(), spark=spark)


def with_metadata(df):
    return df.withColumn("loaded_at", F.current_timestamp()).withColumn("source_file", F.lit("file"))


def finding_names(checked):
    """Names of the failed checks, errors and warnings, one entry per failed row."""
    errors = checked.select(F.explode("_errors").alias("f"))
    warnings = checked.select(F.explode("_warnings").alias("f"))
    return sorted(row.name for row in errors.union(warnings).select("f.name").collect())


def areas(spark, rows):
    return with_metadata(spark.createDataFrame(rows, AREA_SCHEMA))


def test_latest_version_keeps_only_the_newest_file(spark):
    old, new = datetime(2026, 11, 1), datetime(2026, 11, 8)
    df = spark.createDataFrame(
        [("A", "sha=1", old), ("B", "sha=1", old), ("A", "sha=2", new)],
        "hub_bk STRING, source_file STRING, ingested_at TIMESTAMP",
    )

    rows = latest_version(df).collect()

    assert [(r.hub_bk, r.source_file) for r in rows] == [("A", "sha=2")]


def test_to_silver_has_the_csv_columns_then_loaded_at_and_source_file(spark):
    bronze = spark.createDataFrame(
        [("NUS", "U.S.", "country", None, "true", "file", datetime(2026, 11, 1), None, datetime(2026, 11, 1).date())],
        "area_bk STRING, area_name STRING, area_level STRING, parent_area_bk STRING, is_coastal STRING, "
        "source_file STRING, ingested_at TIMESTAMP, _rescued_data STRING, load_date DATE",
    )

    silver = to_silver(bronze, "ref_area")

    assert silver.columns == [*REFERENCE_COLUMNS["ref_area"], "loaded_at", "source_file"]
    assert dict(silver.dtypes)["is_coastal"] == "boolean"


def test_unparsable_is_coastal_becomes_null(spark):
    bronze = spark.createDataFrame(
        [("NUS", "U.S.", "country", None, "maybe", "file", datetime(2026, 11, 1))],
        "area_bk STRING, area_name STRING, area_level STRING, parent_area_bk STRING, is_coastal STRING, "
        "source_file STRING, ingested_at TIMESTAMP",
    )

    assert to_silver(bronze, "ref_area").first().is_coastal is None


def test_to_silver_cleans_values_as_the_stm_says(spark):
    bronze = spark.createDataFrame(
        [(" r10 ", "  PADD 1 ", " Region ", "  ", " TRUE ", "file", datetime(2026, 11, 1))],
        "area_bk STRING, area_name STRING, area_level STRING, parent_area_bk STRING, is_coastal STRING, "
        "source_file STRING, ingested_at TIMESTAMP",
    )

    row = to_silver(bronze, "ref_area").first()

    assert (row.area_bk, row.area_name, row.area_level, row.parent_area_bk, row.is_coastal) == (
        "R10",
        "PADD 1",
        "region",
        None,
        True,
    )


def test_every_reference_column_has_a_cleaning_rule_in_the_bronze_order():
    assert {name: list(kinds) for name, kinds in COLUMN_KINDS.items()} == REFERENCE_COLUMNS


def test_every_reference_table_has_checks_with_names_of_its_own_table():
    assert set(CHECKS) == set(REFERENCE_COLUMNS)
    for table, checks in CHECKS.items():
        names = [c["name"] for c in checks]
        assert all(name.startswith(f"{table}.") for name in names)
        assert len(names) == len(set(names))


def test_clean_areas_have_no_findings(spark, engine):
    df = areas(spark, [("NUS", "U.S.", "country", None, True), ("R10", "PADD 1", "region", "NUS", True)])

    checked = check(df, CHECKS["ref_area"], engine, lookup_tables(df))

    assert quarantine(checked).count() == 0


def test_area_rules_report_the_broken_rule(spark, engine):
    df = areas(
        spark,
        [
            ("NUS", "U.S.", "country", None, True),
            ("R10", "PADD 1", "region", None, True),  # a region without a parent
            ("R20", "PADD 2", "region", "XXX", True),  # parent does not exist
            ("R30", "PADD 3", "planet", "NUS", True),  # unknown level
        ],
    )

    checked = check(df, CHECKS["ref_area"], engine, lookup_tables(df))

    assert finding_names(checked) == [
        "ref_area.area_level.allowed_values",
        "ref_area.parent_area_bk.exists",
        "ref_area.parent_area_bk.null_only_for_country",
    ]


def test_duplicate_key_is_an_error_on_every_copy(spark, engine):
    df = areas(spark, [("NUS", "U.S.", "country", None, True), ("NUS", "U.S.", "country", None, True)])

    checked = check(df, CHECKS["ref_area"], engine, lookup_tables(df))

    assert finding_names(checked) == ["ref_area.area_bk.unique"] * 2


def test_hub_with_unknown_served_area_is_only_a_warning(spark, engine):
    area_df = areas(spark, [("R10", "PADD 1", "region", "NUS", True)])
    hubs = with_metadata(
        spark.createDataFrame(
            [("Y35NY", "New York Harbor", "R99")], "hub_bk STRING, hub_name STRING, served_area_bk STRING"
        )
    )

    checked = check(hubs, CHECKS["ref_hub"], engine, lookup_tables(area_df))

    assert checked.filter(F.col("_errors").isNotNull()).count() == 0
    assert finding_names(checked) == ["ref_hub.served_area_bk.exists"]


def test_product_without_a_unit_is_an_error(spark, engine):
    df = with_metadata(
        spark.createDataFrame(
            [("EPM0F", "Gasoline", "Gasoline", None, None)],
            "product_bk STRING, product_name STRING, product_family STRING, unit STRING, "
            "representative_grade_bk STRING",
        )
    )

    checked = check(df, CHECKS["ref_product"], engine)

    assert finding_names(checked) == ["ref_product.unit.not_null"]


def test_terminal_zone_in_a_non_coastal_area_is_a_warning(spark, engine):
    area_df = areas(spark, [("R20", "PADD 2", "region", "NUS", False)])
    zones = with_metadata(
        spark.createDataFrame(
            [("BOS", "Boston", "product terminal", "R20", "POLYGON((0 0, 1 0, 1 1, 0 0))")],
            "port_bk STRING, port_name STRING, terminal_type STRING, area_bk STRING, boundary_wkt STRING",
        )
    )

    checked = check(zones, CHECKS["ref_terminal_zone"], engine, lookup_tables(area_df))

    assert finding_names(checked) == ["ref_terminal_zone.area_bk.coastal_area"]


def test_mid_must_be_three_digits_starting_2_to_7(spark, engine):
    df = with_metadata(
        spark.createDataFrame([("201", "Albania"), ("801", "X"), ("20", "Y")], "mid STRING, country_name STRING")
    )

    checked = check(df, CHECKS["ref_mid"], engine)

    bad = quarantine(checked).select("mid").collect()
    assert sorted(r.mid for r in bad) == ["20", "801"]


def test_eia_product_must_point_to_a_known_fuel_grade(spark, engine):
    grades = spark.createDataFrame([("REGULAR",)], "grade_bk STRING")
    products = with_metadata(
        spark.createDataFrame(
            [("EPMR", "retail", "REGULAR"), ("EPXX", "retail", "NOPE")],
            "eia_product STRING, price_type STRING, grade_bk STRING",
        )
    )

    checked = check(products, CHECKS["ref_eia_product"], engine, {"ref_fuel_grade": grades})

    assert finding_names(checked) == ["ref_eia_product.grade_bk.exists"]


def test_integrity_finds_unknown_and_foreign_grades(spark):
    products = with_metadata(
        spark.createDataFrame(
            [("P1", "G1"), ("P2", "G1"), ("P3", "GX"), ("P4", None)],
            "product_bk STRING, representative_grade_bk STRING",
        )
    )
    grades = with_metadata(
        spark.createDataFrame([("G1", "P1"), ("G2", "PX"), ("G3", None)], "grade_bk STRING, product_bk STRING")
    )

    findings = integrity_findings(products, grades)

    assert findings.columns == ["check_name", "table_name", "key_value", "bad_value", "level", "loaded_at"]
    assert sorted((r.check_name, r.key_value, r.bad_value) for r in findings.collect()) == [
        ("ref_fuel_grade.product_bk.exists", "G2", "PX"),
        ("ref_product.representative_grade_bk.exists_and_belongs_to_product", "P2", "G1"),
        ("ref_product.representative_grade_bk.exists_and_belongs_to_product", "P3", "GX"),
    ]


def test_only_the_terminal_zone_pipeline_adds_the_geometry_check():
    boundary = [c["name"] for c in pipeline_checks("ref_terminal_zone") if c["check"]["function"] == "is_ogc_valid"]

    assert boundary == ["ref_terminal_zone.boundary_wkt.valid_geometry"]
    assert pipeline_checks("ref_area") == CHECKS["ref_area"]
