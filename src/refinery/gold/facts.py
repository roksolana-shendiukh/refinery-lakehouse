"""Gold facts: one function per fact; keys come from the same functions the dimensions use."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from refinery.gold.dimensions import date_sk, status_sk
from refinery.gold.keys import NOT_APPLICABLE, UNKNOWN, lookup_sk


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


def latest_price(eia_price: DataFrame) -> DataFrame:
    """The latest EIA revision of every week, grade, area and hub: max_by on loaded_at, no window sort."""
    return (
        eia_price.groupBy("report_week", "grade_bk", "area_bk", "hub_bk")
        .agg(
            F.max_by(F.struct("price_type", "price_usd_gal", "value_status", "series_id"), "loaded_at").alias("latest")
        )
        .select("report_week", "grade_bk", "area_bk", "hub_bk", "latest.*")
    )


def fact_price(
    eia_price: DataFrame,
    ref_fuel_grade: DataFrame,
    ref_area: DataFrame,
    ref_hub: DataFrame,
    ref_product: DataFrame,
) -> DataFrame:
    """One row per grade, market and week: the latest EIA revision in $/gal."""
    grades = ref_fuel_grade.select(
        F.col("grade_bk").alias("g_bk"),
        F.col("product_bk").alias("g_product"),
        F.col("source_unit").alias("g_unit"),
    ).dropDuplicates(["g_bk"])
    with_grade = latest_price(eia_price).join(F.broadcast(grades), F.col("grade_bk") == F.col("g_bk"), "left")

    keyed = lookup_sk(with_grade, ref_fuel_grade, "grade_bk", "grade_bk", "grade_sk")
    keyed = lookup_sk(keyed, ref_area, "area_bk", "area_bk", "area_sk")
    keyed = lookup_sk(keyed, ref_hub, "hub_bk", "hub_bk", "hub_sk")
    keyed = lookup_sk(keyed, ref_product, "g_product", "product_bk", "product_sk")

    not_applicable = F.lit(NOT_APPLICABLE).cast("bigint")
    unknown = F.lit(UNKNOWN).cast("bigint")
    is_crude = F.col("g_product").isNull() & (F.col("g_unit") == "$/BBL")

    return keyed.select(
        date_sk(F.col("report_week")).alias("date_sk"),
        "grade_sk",
        F.when(F.col("area_bk").isNull(), not_applicable).otherwise(F.col("area_sk")).alias("area_sk"),
        F.when(F.col("price_type") == "retail", not_applicable).otherwise(F.col("hub_sk")).alias("hub_sk"),
        F.when(F.col("g_bk").isNull(), unknown)
        .when(is_crude, not_applicable)
        .otherwise(F.col("product_sk"))
        .alias("product_sk"),
        status_sk(F.col("price_type"), F.col("value_status")).alias("status_sk"),
        "series_id",
        F.col("price_usd_gal").alias("price"),
    )
