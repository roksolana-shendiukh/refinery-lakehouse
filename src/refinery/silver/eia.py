"""Silver EIA tables: typed weekly supply and prices from bronze, as the STM says."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from refinery.silver.quality import exists_in, expression, not_null

SUPPLY_CHECKS = [
    not_null("eia_supply", "report_week", "error"),
    expression(
        "eia_supply", "report_week", "friday", "error", "dayofweek(report_week) = 6", "report_week must be a Friday"
    ),
    exists_in("eia_supply", "area_bk", "ref_area", "area_bk", "warn"),
    exists_in("eia_supply", "product_bk", "ref_product", "product_bk", "warn"),
    expression(
        "eia_supply",
        "rate_kbd",
        "status_matches_value",
        "error",
        "(value_status = 'ok') = (rate_kbd IS NOT NULL)",
        "rate_kbd must be set exactly when value_status is ok",
    ),
    expression(
        "eia_supply",
        "rate_kbd",
        "not_negative",
        "error",
        "rate_kbd IS NULL OR rate_kbd >= 0",
        "rate_kbd must not be negative",
    ),
    expression("eia_supply", "value_status", "ok", "warn", "value_status = 'ok'", "value is withheld or missing"),
    not_null("eia_supply", "loaded_at", "error"),
    not_null("eia_supply", "source_file", "warn"),
]


def to_supply(df: DataFrame) -> DataFrame:
    """Silver eia_supply columns from eia_supply_raw."""
    value = F.trim(F.col("value"))
    rate = value.try_cast("decimal(10,1)")
    status = F.when(rate.isNotNull(), "ok").when(F.upper(value) == "W", "withheld").otherwise("missing")
    return df.select(
        F.to_date(F.try_to_timestamp(F.col("period"), F.lit("yyyy-MM-dd"))).alias("report_week"),
        F.upper(F.trim(F.col("duoarea"))).alias("area_bk"),
        F.col("product").alias("product_bk"),
        rate.alias("rate_kbd"),
        status.alias("value_status"),
        F.col("ingested_at").alias("loaded_at"),
        "source_file",
    )
