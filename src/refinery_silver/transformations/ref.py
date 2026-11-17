"""Silver reference tables, their quarantine tables and the ref_integrity check view."""

from pyspark import pipelines as dp

from refinery.bronze.reference import REFERENCE_COLUMNS
from refinery.silver.quality import build_engine, check, quarantine, valid
from refinery.silver.reference import integrity_findings, lookup_tables, pipeline_checks, to_silver

# Schemas as <catalog>.<schema>, set in the pipeline configuration
BRONZE = spark.conf.get("refinery.bronze_schema")
QUALITY = spark.conf.get("refinery.quality_schema")

ENGINE = build_engine(spark)


def typed(name):
    """Latest bronze version of a reference table in the silver columns, before any check."""
    return to_silver(spark.read.table(f"{BRONZE}.{name}_raw"), name)


def lookups(name):
    """Tables a reference table is checked against; the silver ones are read inside this pipeline."""
    if name == "ref_area":
        # A parent must be an area of the same file; the unchecked rows avoid a table that reads itself.
        return {"ref_area": typed("ref_area")}
    if name == "ref_hub":
        return {"ref_area": spark.read.table("ref_area")}
    if name == "ref_terminal_zone":
        return lookup_tables(spark.read.table("ref_area"))
    if name == "ref_eia_product":
        return {"ref_fuel_grade": spark.read.table("ref_fuel_grade")}
    return {}


def define_reference_tables(name):
    checked_view = f"{name}_checked"

    @dp.temporary_view(name=checked_view)
    def checked():
        return check(typed(name), pipeline_checks(name), ENGINE, lookups(name))

    @dp.materialized_view(name=name, comment=f"Reference table {name}: latest version of the CSV, rows without errors")
    def silver():
        return valid(spark.read.table(checked_view))

    @dp.materialized_view(
        name=f"{QUALITY}.{name}_quarantine",
        comment=f"Rows of {name} with an error or a warning, with the DQX result columns",
    )
    def quarantined():
        return quarantine(spark.read.table(checked_view))


for name in REFERENCE_COLUMNS:
    define_reference_tables(name)


@dp.materialized_view(
    name=f"{QUALITY}.ref_integrity",
    comment="Cross-checks between ref_product and ref_fuel_grade, one row per problem (warnings)",
)
def ref_integrity():
    return integrity_findings(spark.read.table("ref_product"), spark.read.table("ref_fuel_grade"))
