"""Silver AIS tables: PositionReport and ShipStaticData messages from the landing table, as the STM says."""

from pyspark.sql import Column, DataFrame, Window
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


def positions_in_zones(positions: DataFrame, zones: DataFrame, in_zone: Column) -> DataFrame:
    """Positions inside a terminal zone, with its port_bk; positions outside every zone are dropped.

    Only the columns the join needs are taken from the zones: ref_terminal_zone has its own loaded_at,
    which would clash with the loaded_at of the position.
    """
    return positions.join(zones.select("port_bk", "boundary_wkt"), in_zone).select(
        "mmsi", "event_ts", "port_bk", "sog", "nav_status", "loaded_at"
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


def to_vessel_mmsi_map(static: DataFrame, positions: DataFrame) -> DataFrame:
    """Which MMSI a vessel (IMO) used and when: one row per IMO-MMSI pair, closed by the vessel's next MMSI."""
    pairs = (
        static.where(F.col("imo").isNotNull() & F.col("mmsi").isNotNull())
        .groupBy("imo", "mmsi")
        .agg(F.min("event_ts").alias("first_static_ts"), F.max("loaded_at").alias("loaded_at"))
    )
    first_seen = positions.groupBy("mmsi").agg(F.min("event_ts").alias("first_seen"))
    by_vessel = Window.partitionBy("imo").orderBy("effective_from")
    return (
        pairs.join(first_seen, "mmsi", "left")
        # least() skips NULL, so a vessel without positions starts at its first static message.
        .withColumn("effective_from", F.least("first_static_ts", "first_seen"))
        .withColumn("effective_to", F.lead("effective_from").over(by_vessel))
        .select("imo", "mmsi", "effective_from", "effective_to", "first_static_ts", "loaded_at")
    )


MMSI_MAP_CHECKS = [
    _in_range("vessel_mmsi_map", "imo", 1000000, 9999999, "warn"),
    not_null("vessel_mmsi_map", "mmsi", "error"),
    _nine_digit_mmsi("vessel_mmsi_map"),
    not_null("vessel_mmsi_map", "effective_from", "error"),
    expression(
        "vessel_mmsi_map",
        "effective_from",
        "unique",
        "error",
        "mmsi IS NULL OR effective_from IS NULL OR count(*) OVER (PARTITION BY mmsi, effective_from) = 1",
        "mmsi and effective_from must be unique",
    ),
    expression(
        "vessel_mmsi_map",
        "effective_from",
        "not_after_first_static",
        "error",
        "effective_from IS NULL OR first_static_ts IS NULL OR effective_from <= first_static_ts",
        "effective_from must not be later than first_static_ts",
    ),
    expression(
        "vessel_mmsi_map",
        "effective_to",
        "after_effective_from",
        "error",
        "effective_to IS NULL OR effective_to > effective_from",
        "effective_to must be later than effective_from",
    ),
    expression(
        "vessel_mmsi_map",
        "effective_to",
        "one_open_per_vessel",
        "error",
        "imo IS NULL OR count_if(effective_to IS NULL) OVER (PARTITION BY imo) = 1",
        "each vessel must have exactly one row with effective_to NULL",
    ),
    not_null("vessel_mmsi_map", "first_static_ts", "error"),
    not_null("vessel_mmsi_map", "loaded_at", "error"),
]