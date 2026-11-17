"""Silver AIS tables and their quarantine tables. Runs in PROD only: the landing table exists only there."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from refinery.silver.ais import (
    MMSI_MAP_CHECKS,
    POSITION_CHECKS,
    STATIC_CHECKS,
    to_positions,
    to_static,
    to_vessel_mmsi_map,
)
from refinery.silver.quality import build_engine, check, quarantine, valid

SOURCE = spark.conf.get("refinery.source_table")
# Schema as <catalog>.<schema>, set in the pipeline configuration
QUALITY = spark.conf.get("refinery.quality_schema")

ENGINE = build_engine(spark)


def landing(message_type):
    return spark.readStream.table(SOURCE).where(F.col("message_type") == message_type)


@dp.temporary_view(name="ais_zone_positions_checked")
def ais_zone_positions_checked():
    zones = spark.read.table("ref_terminal_zone")
    in_zone = F.expr("st_contains(st_geomfromwkt(boundary_wkt), st_point(longitude, latitude))")
    # Inner join: positions outside every terminal zone are not kept.
    positions = (
        to_positions(landing("PositionReport"))
        .join(zones, in_zone)
        .select("mmsi", "event_ts", "port_bk", "sog", "nav_status", "loaded_at")
    )
    return check(positions, POSITION_CHECKS, ENGINE)


@dp.temporary_view(name="ais_vessel_static_checked")
def ais_vessel_static_checked():
    return check(to_static(landing("ShipStaticData")), STATIC_CHECKS, ENGINE)


@dp.temporary_view(name="vessel_mmsi_map_checked")
def vessel_mmsi_map_checked():
    vessel_map = to_vessel_mmsi_map(spark.read.table("ais_vessel_static"), spark.read.table("ais_zone_positions"))
    return check(vessel_map, MMSI_MAP_CHECKS, ENGINE)


def define_stream_table(name, checked_view, comment):
    @dp.table(name=name, comment=comment, cluster_by=["mmsi", "event_ts"])
    def silver():
        return valid(spark.readStream.table(checked_view))

    @dp.table(
        name=f"{QUALITY}.{name}_quarantine",
        comment=f"Rows of {name} with an error or a warning, with the DQX result columns",
        cluster_by=["mmsi", "event_ts"],
    )
    def quarantined():
        return quarantine(spark.readStream.table(checked_view))


define_stream_table(
    "ais_zone_positions",
    "ais_zone_positions_checked",
    "AIS position reports inside a terminal zone, one row per message; rows without errors",
)
define_stream_table(
    "ais_vessel_static",
    "ais_vessel_static_checked",
    "AIS static data messages, one row per message; rows without errors",
)


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
