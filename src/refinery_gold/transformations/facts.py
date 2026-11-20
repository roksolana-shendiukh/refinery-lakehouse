"""Gold facts as materialized views; the logic lives in refinery.gold.facts."""

from pyspark import pipelines as dp

from refinery.gold.config import silver
from refinery.gold.facts import fact_supply
from refinery.gold.schemas import FACT_SUPPLY_DDL


@dp.materialized_view(
    name="fact_periodic_supply_weekly",
    schema=FACT_SUPPLY_DDL,
    comment="Weekly U.S. product supplied (thousand barrels per day): one row per product, area and week, "
    "latest EIA revision; rate_kbd is semi-additive, average it over time",
    cluster_by=["date_sk", "area_sk"],
)
def fact_supply_table():
    return fact_supply(silver(spark, "eia_supply"), silver(spark, "ref_area"), silver(spark, "ref_product"))
