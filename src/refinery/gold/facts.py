"""Gold facts: one function per fact; keys come from the same functions the dimensions use."""

from datetime import datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from refinery.gold.dimensions import date_sk, status_sk, time_sk
from refinery.gold.keys import NOT_APPLICABLE, NOT_YET, UNKNOWN, lookup_sk


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


CALL_GAP = "INTERVAL 6 HOURS"
MOORED_NAV_STATUS = 5
MOORED_MAX_SOG = 0.5
ARRIVAL_DRAUGHT_WINDOW = "INTERVAL 1 HOUR"


def port_calls(positions: DataFrame, dim_vessel: DataFrame, now: datetime) -> DataFrame:
    """One row per call (gaps and islands): positions of one MMSI in one zone with no gap over 6 hours."""
    gap = F.expr(CALL_GAP)
    tankers = dim_vessel.select("mmsi").where(F.col("mmsi").isNotNull()).distinct()
    points = positions.select("mmsi", "port_bk", "event_ts", "sog", "nav_status").join(
        F.broadcast(tankers), "mmsi", "left_semi"
    )

    in_zone = Window.partitionBy("mmsi", "port_bk").orderBy("event_ts")
    previous = F.lag("event_ts").over(in_zone)
    starts_a_call = (previous.isNull() | (F.col("event_ts") > previous + gap)).cast("int")
    numbered = points.withColumn("starts_a_call", starts_a_call).withColumn(
        "call_no", F.sum("starts_a_call").over(in_zone)
    )

    is_moored = (F.col("nav_status") == MOORED_NAV_STATUS) | (F.col("sog") < MOORED_MAX_SOG)
    calls = numbered.groupBy("mmsi", "port_bk", "call_no").agg(
        F.min("event_ts").alias("entered_ts"),
        F.min(F.when(is_moored, F.col("event_ts"))).alias("moored_ts"),
        F.max("event_ts").alias("last_ts"),
    )
    left = F.lit(now) > F.col("last_ts") + gap
    return calls.select(
        "mmsi", "port_bk", "entered_ts", "moored_ts", F.when(left, F.col("last_ts")).alias("departed_ts")
    )


def _with_arrival_draught(calls: DataFrame, static: DataFrame) -> DataFrame:
    """Each call with the draught reported around its arrival: the latest value that started up to an hour after entry."""
    draughts = static.where(F.col("draught").isNotNull()).select(
        F.col("mmsi").alias("d_mmsi"), F.col("__START_AT").alias("d_from"), "draught"
    )
    around_arrival = (F.col("mmsi") == F.col("d_mmsi")) & (
        F.col("d_from") <= F.col("entered_ts") + F.expr(ARRIVAL_DRAUGHT_WINDOW)
    )
    return (
        calls.join(draughts, around_arrival, "left")
        .groupBy(*calls.columns)
        .agg(F.max_by("draught", "d_from").alias("arrival_draught"))
    )


def _hours(end: Column, start: Column) -> Column:
    """Hours between two timestamps as DECIMAL(8,2)."""
    return ((F.unix_timestamp(end) - F.unix_timestamp(start)) / 3600).cast("decimal(8,2)")


def fact_port_call(
    positions: DataFrame,
    dim_vessel: DataFrame,
    static: DataFrame,
    ref_terminal_zone: DataFrame,
    ref_area: DataFrame,
    now: datetime,
) -> DataFrame:
    """One row per call of one tanker at one terminal zone (accumulating snapshot): entered, moored, departed."""
    calls = _with_arrival_draught(port_calls(positions, dim_vessel, now), static)

    versions = dim_vessel.where(F.col("mmsi").isNotNull()).select(
        F.col("mmsi").alias("v_mmsi"),
        F.col("effective_from").alias("v_from"),
        F.col("effective_to").alias("v_to"),
        "vessel_sk",
    )
    valid_at_entry = (
        (F.col("mmsi") == F.col("v_mmsi"))
        & (F.col("entered_ts") >= F.col("v_from"))
        & (F.col("v_to").isNull() | (F.col("entered_ts") < F.col("v_to")))
    )
    with_vessel = calls.join(F.broadcast(versions), valid_at_entry, "inner")

    zones = ref_terminal_zone.select(F.col("port_bk").alias("z_port"), F.col("area_bk").alias("z_area"))
    with_zone = with_vessel.join(F.broadcast(zones), F.col("port_bk") == F.col("z_port"), "left")
    keyed = lookup_sk(with_zone, ref_terminal_zone, "port_bk", "port_bk", "port_sk")
    keyed = lookup_sk(keyed, ref_area, "z_area", "area_bk", "area_sk")

    entered, moored, departed = F.col("entered_ts"), F.col("moored_ts"), F.col("departed_ts")
    not_yet = F.lit(NOT_YET)
    not_moored = F.when(departed.isNotNull(), F.lit(NOT_APPLICABLE)).otherwise(not_yet)

    return keyed.select(
        "vessel_sk",
        "port_sk",
        date_sk(entered).alias("entered_date_sk"),
        time_sk(entered).alias("entered_time_sk"),
        F.coalesce(date_sk(moored), not_moored).alias("moored_date_sk"),
        F.coalesce(time_sk(moored), not_moored).alias("moored_time_sk"),
        F.coalesce(date_sk(departed), not_yet).alias("departed_date_sk"),
        F.coalesce(time_sk(departed), not_yet).alias("departed_time_sk"),
        "area_sk",
        _hours(F.coalesce(moored, departed), entered).alias("wait_hours"),
        F.when(moored.isNotNull() & departed.isNotNull(), _hours(departed, moored)).alias("berth_hours"),
        "arrival_draught",
    )