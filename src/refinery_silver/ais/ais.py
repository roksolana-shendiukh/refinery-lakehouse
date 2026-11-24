"""Silver AIS tables and their quarantine tables. Runs in PROD only: the landing table exists only there."""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from refinery.silver.ais import (
    MMSI_MAP_CHECKS,
    POSITION_CHECKS,
    STATIC_CHECKS,
    positions_in_zones,
    to_positions,
    to_static,
    to_vessel_mmsi_map,
)
from refinery.silver.metrics import build_metrics_engine, summary_metrics
from refinery.silver.quality import build_engine, check, quarantine, valid

SOURCE = spark.conf.get("refinery.source_table")
# Schema as <catalog>.<schema>, set in the pipeline configuration
QUALITY = spark.conf.get("refinery.quality_schema")

# A repeated position (same MMSI and event time) is dropped when it arrives within this time of the first one.
DUPLICATE_WINDOW = "3 hours"

ENGINE = build_engine(spark)

# A position is inside a terminal zone when the zone polygon contains its point
IN_ZONE = "st_contains(st_geomfromwkt(boundary_wkt), st_point(longitude, latitude))"


def landing(message_type):
    return spark.readStream.table(SOURCE).where(F.col("message_type") == message_type)


def define_quarantine(name, checked_view, cluster_by):
    @dp.table(
        name=f"{QUALITY}.{name}_quarantine",
        comment=f"Rows of {name} with an error or a warning, with the DQX result columns",
        cluster_by=cluster_by,
    )
    def quarantined():
        return quarantine(spark.readStream.table(checked_view))


@dp.temporary_view(name="ais_zone_positions_checked")
def ais_zone_positions_checked():
    zones = spark.read.table("ref_terminal_zone")
    in_zone = F.expr(IN_ZONE)
    # Inner join: positions outside every terminal zone are not kept.
    positions = positions_in_zones(to_positions(landing("PositionReport")), zones, in_zone)
    checked = check(positions, POSITION_CHECKS, ENGINE)
    return checked.withWatermark("event_ts", DUPLICATE_WINDOW).dropDuplicatesWithinWatermark(["mmsi", "event_ts"])


@dp.table(
    name="ais_zone_positions",
    comment="AIS position reports inside a terminal zone, one row per MMSI and event time; rows without errors",
    cluster_by=["mmsi", "event_ts"],
)
def ais_zone_positions():
    return valid(spark.readStream.table("ais_zone_positions_checked"))


define_quarantine("ais_zone_positions", "ais_zone_positions_checked", ["mmsi", "event_ts"])


@dp.temporary_view(name="ais_vessel_static_checked")
def ais_vessel_static_checked():
    return check(to_static(landing("ShipStaticData")), STATIC_CHECKS, ENGINE)


@dp.temporary_view(name="ais_vessel_static_valid")
def ais_vessel_static_valid():
    return valid(spark.readStream.table("ais_vessel_static_checked"))


# Change log (SCD2): a new version of a vessel starts when its IMO, name, type, length or draught changes.
dp.create_streaming_table(
    name="ais_vessel_static",
    comment="AIS static data as a change log per MMSI (SCD2); rows without errors",
    cluster_by=["mmsi"],
)
dp.create_auto_cdc_flow(
    target="ais_vessel_static",
    source="ais_vessel_static_valid",
    keys=["mmsi"],
    sequence_by="event_ts",
    stored_as_scd_type=2,
    track_history_column_list=["imo", "vessel_name", "ship_type", "length_m", "draught"],
)

define_quarantine("ais_vessel_static", "ais_vessel_static_checked", ["mmsi", "event_ts"])


@dp.temporary_view(name="vessel_mmsi_map_checked")
def vessel_mmsi_map_checked():
    # The change log starts a new version at __START_AT: that is the first time the vessel used this IMO and MMSI.
    static = spark.read.table("ais_vessel_static").select(
        "mmsi", F.col("__START_AT").alias("event_ts"), "imo", "loaded_at"
    )
    vessel_map = to_vessel_mmsi_map(static, spark.read.table("ais_zone_positions"))
    return check(vessel_map, MMSI_MAP_CHECKS, ENGINE)


@dp.materialized_view(
    name="vessel_mmsi_map",
    comment="Which MMSI a vessel (IMO) used and when: one row per IMO and MMSI; rows without errors",
    cluster_by=["mmsi"],
)
def vessel_mmsi_map():
    return valid(spark.read.table("vessel_mmsi_map_checked"))


@dp.materialized_view(
    name=f"{QUALITY}.vessel_mmsi_map_quarantine",
    comment="Rows of vessel_mmsi_map with an error or a warning, with the DQX result columns",
    cluster_by=["mmsi"],
)
def vessel_mmsi_map_quarantine():
    return quarantine(spark.read.table("vessel_mmsi_map_checked"))


# --- Tables for the data-quality dashboard ---

METRICS_ENGINE = build_metrics_engine(spark)

# vessel_mmsi_map changes only when a new IMO-MMSI pair appears, so its age says nothing about the producer
FRESHNESS_TABLES = ["ais_zone_positions", "ais_vessel_static"]


def positions_for_metrics():
    """Checked positions as a batch over the landing table, so the metrics do not read the streaming views.

    Same steps as ais_zone_positions_checked: the zone join, the checks, the deduplication.
    """
    landed = spark.read.table(SOURCE).where(F.col("message_type") == "PositionReport")
    positions = positions_in_zones(to_positions(landed), spark.read.table("ref_terminal_zone"), F.expr(IN_ZONE))
    return check(positions, POSITION_CHECKS, ENGINE).dropDuplicates(["mmsi", "event_ts"])


def static_for_metrics():
    """Checked static messages as a batch over the landing table: input rows are messages, not SCD2 versions."""
    landed = spark.read.table(SOURCE).where(F.col("message_type") == "ShipStaticData")
    return check(to_static(landed), STATIC_CHECKS, ENGINE)


@dp.materialized_view(
    name=f"{QUALITY}.dq_summary_metrics_latest_ais",
    comment="DQX summary metrics of the last AIS silver run, one row per metric and table",
)
def dq_summary_metrics_latest_ais():
    checked = {
        "ais_zone_positions": (positions_for_metrics(), POSITION_CHECKS),
        "ais_vessel_static": (static_for_metrics(), STATIC_CHECKS),
        "vessel_mmsi_map": (spark.read.table("vessel_mmsi_map_checked"), MMSI_MAP_CHECKS),
    }
    metrics = [summary_metrics(df, checks, METRICS_ENGINE, table) for table, (df, checks) in checked.items()]
    return reduce(DataFrame.unionByName, metrics)


@dp.materialized_view(
    name=f"{QUALITY}.dq_freshness_ais",
    comment="Latest load and row count of the AIS silver tables that the producer feeds",
)
def dq_freshness_ais():
    tables = [
        spark.read.table(table)
        .agg(F.max("loaded_at").alias("last_loaded_at"), F.count("*").alias("row_count"))
        .select(F.lit(table).alias("table_name"), "last_loaded_at", "row_count")
        for table in FRESHNESS_TABLES
    ]
    return reduce(DataFrame.unionByName, tables)
