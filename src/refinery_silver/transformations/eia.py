"""Silver EIA tables and their quarantine tables."""

from pyspark import pipelines as dp

from refinery.silver.eia import PRICE_CHECKS, SUPPLY_CHECKS, in_scope, to_price, to_supply
from refinery.silver.quality import build_engine, check, quarantine, valid

# Schemas as <catalog>.<schema>, set in the pipeline configuration
BRONZE = spark.conf.get("refinery.bronze_schema")
QUALITY = spark.conf.get("refinery.quality_schema")

ENGINE = build_engine(spark)


@dp.temporary_view(name="eia_supply_checked")
def eia_supply_checked():
    lookups = {"ref_area": spark.read.table("ref_area"), "ref_product": spark.read.table("ref_product")}
    supply = to_supply(spark.read.table(f"{BRONZE}.eia_supply_raw"))
    return check(supply, SUPPLY_CHECKS, ENGINE, lookups)


@dp.temporary_view(name="eia_price_checked")
def eia_price_checked():
    prices = to_price(
        spark.read.table(f"{BRONZE}.eia_price_raw"),
        spark.read.table("ref_eia_product"),
        spark.read.table("ref_hub"),
    )
    # The scope filter runs before the checks: retail rows outside ref_area are not quarantined.
    scoped = in_scope(prices, spark.read.table("ref_area"))
    checked = check(scoped, PRICE_CHECKS, ENGINE, {"ref_hub": spark.read.table("ref_hub")})
    return checked.drop("source_units")


def define_eia_table(name, checked_view, comment):
    @dp.materialized_view(name=name, comment=comment, cluster_by=["report_week"])
    def silver():
        return valid(spark.read.table(checked_view))

    @dp.materialized_view(
        name=f"{QUALITY}.{name}_quarantine",
        comment=f"Rows of {name} with an error or a warning, with the DQX result columns",
        cluster_by=["report_week"],
    )
    def quarantined():
        return quarantine(spark.read.table(checked_view))


define_eia_table(
    "eia_supply",
    "eia_supply_checked",
    "EIA weekly product supplied: one row per week, area, product and load; rows without errors",
)
define_eia_table(
    "eia_price",
    "eia_price_checked",
    "EIA weekly retail and spot prices: one row per week, price type, series and load; "
    "retail only for areas in ref_area; rows without errors",
)
