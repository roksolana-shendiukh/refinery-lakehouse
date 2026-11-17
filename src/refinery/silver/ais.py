"""Silver AIS tables: PositionReport and ShipStaticData messages from the landing table, as the STM says."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from refinery.silver.quality import expression, not_null


def _field(path: str, type_: str) -> Column:
    """One value of the raw AIS message, NULL when it is missing or has another type."""
    return F.try_variant_get(F.col("raw_message"), path, type_)


def _mmsi() -> Column:
    return _field("$.MetaData.MMSI", "int")


def _event_ts() -> Column:
    # time_utc looks like "2026-11-08 14:27:52.331783091 +0000 UTC": the first 26 characters are the time.
    time_utc = _field("$.MetaData.time_utc", "string")
    return F.try_to_timestamp(F.substring(time_utc, 1, 26), F.lit("yyyy-MM-dd HH:mm:ss.SSSSSS"))


def to_positions(df: DataFrame) -> DataFrame:
    """PositionReport messages in the silver columns, plus latitude and longitude for the zone join."""
    sog = _field("$.Message.PositionReport.Sog", "double")
    nav_status = _field("$.Message.PositionReport.NavigationalStatus", "smallint")
    return df.filter(F.col("message_type") == "PositionReport").select(
        _mmsi().alias("mmsi"),
        _event_ts().alias("event_ts"),
        _field("$.Message.PositionReport.Latitude", "double").alias("latitude"),
        _field("$.Message.PositionReport.Longitude", "double").alias("longitude"),
        # 102.3 knots and navigational status 15 mean "not available" in AIS.
        F.when(sog < 102.3, sog.try_cast("decimal(4,1)")).alias("sog"),
        F.when(nav_status != 15, nav_status).alias("nav_status"),
        F.col("received_at").alias("loaded_at"),
    )


def to_static(df: DataFrame) -> DataFrame:
    """ShipStaticData messages in the silver columns. 0 and blank values mean "not available" in AIS."""
    imo = _field("$.Message.ShipStaticData.ImoNumber", "int")
    name = _field("$.Message.ShipStaticData.Name", "string")
    ship_type = _field("$.Message.ShipStaticData.Type", "smallint")
    to_bow = _field("$.Message.ShipStaticData.Dimension.A", "smallint")
    to_stern = _field("$.Message.ShipStaticData.Dimension.B", "smallint")
    draught = _field("$.Message.ShipStaticData.MaximumStaticDraught", "double")
    return df.filter(F.col("message_type") == "ShipStaticData").select(
        _mmsi().alias("mmsi"),
        _event_ts().alias("event_ts"),
        F.when(imo != 0, imo).alias("imo"),
        F.when(F.trim(name) != "", F.trim(name)).alias("vessel_name"),
        F.when(ship_type != 0, ship_type).alias("ship_type"),
        F.when(to_bow + to_stern > 0, to_bow + to_stern).alias("length_m"),
        F.when(draught > 0, draught.try_cast("decimal(4,1)")).alias("draught"),
        F.col("received_at").alias("loaded_at"),
    )


def _nine_digit_mmsi(table: str) -> dict:
    return expression(
        table,
        "mmsi",
        "nine_digits",
        "error",
        "mmsi IS NULL OR mmsi BETWEEN 100000000 AND 999999999",
        "mmsi must have nine digits",
    )


def _not_in_future(table: str) -> dict:
    return expression(
        table,
        "event_ts",
        "not_in_future",
        "error",
        "event_ts IS NULL OR event_ts <= loaded_at + INTERVAL 5 MINUTES",
        "event_ts must not be later than loaded_at plus 5 minutes",
    )


def _in_range(table: str, column: str, low, high, level: str) -> dict:
    """NULL passes: a missing value is flagged by a not_null check where one is needed."""
    return expression(
        table,
        column,
        "in_range",
        level,
        f"{column} IS NULL OR {column} BETWEEN {low} AND {high}",
        f"{column} must be between {low} and {high}",
    )


POSITION_CHECKS = [
    not_null("ais_zone_positions", "mmsi", "error"),
    _nine_digit_mmsi("ais_zone_positions"),
    not_null("ais_zone_positions", "event_ts", "error"),
    _not_in_future("ais_zone_positions"),
    _in_range("ais_zone_positions", "sog", 0, 102.2, "warn"),
    _in_range("ais_zone_positions", "nav_status", 0, 14, "warn"),
    not_null("ais_zone_positions", "loaded_at", "error"),
]

STATIC_CHECKS = [
    not_null("ais_vessel_static", "mmsi", "error"),
    _nine_digit_mmsi("ais_vessel_static"),
    not_null("ais_vessel_static", "event_ts", "error"),
    _not_in_future("ais_vessel_static"),
    _in_range("ais_vessel_static", "imo", 1000000, 9999999, "warn"),
    not_null("ais_vessel_static", "vessel_name", "warn"),
    _in_range("ais_vessel_static", "ship_type", 1, 99, "warn"),
    _in_range("ais_vessel_static", "length_m", 1, 500, "warn"),
    _in_range("ais_vessel_static", "draught", 0.1, 30, "warn"),
    not_null("ais_vessel_static", "loaded_at", "error"),
]
