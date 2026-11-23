"""Gold AIS tables: tankers and their port calls. PROD only: the silver AIS tables exist only there."""

from datetime import UTC, datetime

from pyspark import pipelines as dp

from refinery.gold.config import silver
from refinery.gold.dimensions import dim_vessel
from refinery.gold.facts import fact_port_call
from refinery.gold.schemas import DIM_VESSEL_DDL, FACT_PORT_CALL_DDL


@dp.materialized_view(
    name="dim_vessel",
    schema=DIM_VESSEL_DDL,
    comment="One version of a tanker (SCD2 on MMSI and flag; Type 1 name, type and length), "
    "plus Unknown / Not applicable / Not yet",
)
def dim_vessel_table():
    return dim_vessel(
        silver(spark, "vessel_mmsi_map"),
        silver(spark, "ais_vessel_static"),
        silver(spark, "ais_zone_positions"),
        silver(spark, "ref_mid"),
    )


@dp.materialized_view(
    name="fact_accumulating_port_call",
    schema=FACT_PORT_CALL_DDL,
    comment="One call of one tanker at one terminal zone (accumulating snapshot): entered, moored, departed; "
    "wait_hours and berth_hours are additive, arrival_draught is not",
    cluster_by=["entered_date_sk", "area_sk"],
)
def fact_port_call_table():
    return fact_port_call(
        silver(spark, "ais_zone_positions"),
        spark.read.table("dim_vessel"),
        silver(spark, "ais_vessel_static"),
        silver(spark, "ref_terminal_zone"),
        silver(spark, "ref_area"),
        datetime.now(UTC),
    )
