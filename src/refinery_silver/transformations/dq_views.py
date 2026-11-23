"""Tables for the data-quality dashboard: freshness of the EIA silver tables and revisions of EIA values."""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from refinery.silver.revisions import revisions

# Schema as <catalog>.<schema>, set in the pipeline configuration
QUALITY = spark.conf.get("refinery.quality_schema")

# Key columns (without loaded_at) and the value that can be revised
REVISED = {
    "eia_supply": (["report_week", "area_bk", "product_bk"], "rate_kbd"),
    "eia_price": (["report_week", "price_type", "series_id"], "price_usd_gal"),
}

# Reference files land only when they change, so their age says nothing about freshness
FRESHNESS_TABLES = list(REVISED)


@dp.materialized_view(
    name=f"{QUALITY}.dq_freshness",
    comment="Latest load and row count of each EIA silver table",
)
def dq_freshness():
    tables = [
        spark.read.table(table)
        .agg(F.max("loaded_at").alias("last_loaded_at"), F.count("*").alias("row_count"))
        .select(F.lit(table).alias("table_name"), "last_loaded_at", "row_count")
        for table in FRESHNESS_TABLES
    ]
    return reduce(DataFrame.unionByName, tables)


@dp.materialized_view(
    name=f"{QUALITY}.dq_revisions",
    comment="Per EIA load: rows that were loaded before, and how many of them changed their value",
)
def dq_revisions():
    tables = [revisions(spark.read.table(table), table, keys, value) for table, (keys, value) in REVISED.items()]
    return reduce(DataFrame.unionByName, tables)
