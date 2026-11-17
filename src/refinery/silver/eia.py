"""Silver EIA tables: typed weekly supply and prices from bronze, as the STM says."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from refinery.silver.quality import exists_in, expression, not_null, one_of

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

PRICE_CHECKS = [
    not_null("eia_price", "report_week", "error"),
    expression(
        "eia_price", "report_week", "friday", "error", "dayofweek(report_week) = 6", "report_week must be a Friday"
    ),
    not_null("eia_price", "price_type", "error"),
    one_of("eia_price", "price_type", ["retail", "spot"], "error"),
    # A NULL hub_bk (retail) is skipped by the foreign key; spot_not_null and retail_null cover it.
    exists_in("eia_price", "hub_bk", "ref_hub", "hub_bk", "warn"),
    expression(
        "eia_price",
        "hub_bk",
        "spot_not_null",
        "error",
        "price_type <> 'spot' OR hub_bk IS NOT NULL",
        "a spot price needs a hub",
    ),
    expression(
        "eia_price",
        "hub_bk",
        "retail_null",
        "error",
        "price_type <> 'retail' OR hub_bk IS NULL",
        "a retail price has no hub",
    ),
    not_null("eia_price", "grade_bk", "error"),
    expression(
        "eia_price",
        "price_usd_gal",
        "source_units",
        "error",
        "COALESCE(source_units IN ('$/GAL', '$/BBL'), FALSE)",
        "source units must be $/GAL or $/BBL",
    ),
    expression(
        "eia_price",
        "price_usd_gal",
        "positive",
        "error",
        "price_usd_gal IS NULL OR price_usd_gal > 0",
        "price must be positive",
    ),
    expression("eia_price", "value_status", "ok", "warn", "value_status = 'ok'", "value is withheld or missing"),
    expression(
        "eia_price",
        "value_status",
        "matches_price",
        "error",
        "(value_status = 'ok') = (price_usd_gal IS NOT NULL)",
        "price_usd_gal must be set exactly when value_status is ok",
    ),
    not_null("eia_price", "series_id", "error"),
    not_null("eia_price", "loaded_at", "error"),
    not_null("eia_price", "source_file", "warn"),
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


def in_scope(price: DataFrame, areas: DataFrame) -> DataFrame:
    """Drop retail rows for areas outside ref_area: our customers work at PADD level and in the listed states.

    This is a scope filter, not a quality check, so the dropped rows are not quarantined. Spot rows and rows
    with an unknown route stay, so the checks can flag them.
    """
    known = areas.select(F.col("area_bk").alias("known_area_bk")).distinct()
    outside = F.coalesce((F.col("price_type") == "retail") & F.col("known_area_bk").isNull(), F.lit(False))
    joined = price.join(F.broadcast(known), price["area_bk"] == known["known_area_bk"], "left")
    return joined.filter(~outside).drop("known_area_bk")
