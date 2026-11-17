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


def to_price(df: DataFrame, eia_products: DataFrame, hubs: DataFrame) -> DataFrame:
    """Silver eia_price columns from eia_price_raw, plus source_units for the units check.

    `eia_products` (eia_product, price_type, grade_bk) and `hubs` (hub_bk, served_area_bk) are the silver
    reference tables. Both joins are left joins, so a missing match gives NULL and the row is kept.
    """
    area = F.upper(F.trim(F.col("duoarea")))
    value = F.trim(F.col("value"))
    amount = value.try_cast("decimal(12,4)")
    units = F.upper(F.trim(F.col("units")))
    week = F.to_date(F.try_to_timestamp(F.col("period"), F.lit("yyyy-MM-dd")))
    status = F.when(amount.isNotNull(), "ok").when(F.upper(value) == "W", "withheld").otherwise("missing")

    typed = (
        df.withColumn("report_week", F.when(F.col("route") == "pri/gnd", F.date_sub(week, 3)).otherwise(week))
        .withColumn(
            "price_type", F.when(F.col("route") == "pri/gnd", "retail").when(F.col("route") == "pri/spt", "spot")
        )
        .withColumn("hub_bk", F.when(F.col("price_type") == "spot", area))
    )
    joined = typed.join(F.broadcast(hubs.select("hub_bk", "served_area_bk")), "hub_bk", "left").join(
        F.broadcast(eia_products.select(F.col("eia_product").alias("product"), "price_type", "grade_bk")),
        ["product", "price_type"],
        "left",
    )
    price_per_gallon = F.when(units == "$/GAL", amount).when(units == "$/BBL", amount / 42)
    return joined.select(
        "report_week",
        "price_type",
        F.when(F.col("price_type") == "retail", area).otherwise(F.col("served_area_bk")).alias("area_bk"),
        "hub_bk",
        "grade_bk",
        price_per_gallon.try_cast("decimal(9,3)").alias("price_usd_gal"),
        status.alias("value_status"),
        F.trim(F.col("series")).alias("series_id"),
        F.col("ingested_at").alias("loaded_at"),
        "source_file",
        units.alias("source_units"),
    )
