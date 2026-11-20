"""Gold facts: one function per fact; keys come from the same functions the dimensions use."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from refinery.gold.dimensions import date_sk, status_sk
from refinery.gold.keys import lookup_sk


def latest_supply(eia_supply: DataFrame) -> DataFrame:
    """The latest EIA revision of every week, area and product."""
    return (
        eia_supply.groupBy("report_week", "area_bk", "product_bk")
        .agg(F.max_by(F.struct("rate_kbd", "value_status"), "loaded_at").alias("latest"))
        .select("report_week", "area_bk", "product_bk", "latest.rate_kbd", "latest.value_status")
    )


def fact_supply(eia_supply: DataFrame, ref_area: DataFrame, ref_product: DataFrame) -> DataFrame:
    """One row per product, area and week."""
    latest = latest_supply(eia_supply)
    with_area = lookup_sk(latest, ref_area, "area_bk", "area_bk", "area_sk")
    with_keys = lookup_sk(with_area, ref_product, "product_bk", "product_bk", "product_sk")
    return with_keys.select(
        date_sk(F.col("report_week")).alias("date_sk"),
        "area_sk",
        "product_sk",
        status_sk(F.lit("n.a."), F.col("value_status")).alias("status_sk"),
        "rate_kbd",
    )
