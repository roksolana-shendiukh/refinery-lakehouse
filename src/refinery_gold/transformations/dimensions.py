"""Gold dimensions as materialized views; the logic lives in refinery.gold.dimensions."""

from datetime import UTC, datetime, timedelta

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from refinery.gold.config import silver
from refinery.gold.dimensions import calendar_start, dim_date, dim_status
from refinery.gold.schemas import DIM_DATE_DDL, DIM_STATUS_DDL

SILVER = spark.conf.get("refinery.silver_schema")


@dp.materialized_view(
    name="dim_status",
    schema=DIM_STATUS_DDL,
    comment="Junk dimension: every price type with every value status, plus Unknown / Not applicable / Not yet",
)
def dim_status_table():
    return dim_status(spark)


def fact_dates():
    """Dates the facts use; AIS positions exist only on PROD, so they count only when the table is there."""
    dates = [
        silver(spark, "eia_supply").select(F.col("report_week").alias("date")),
        silver(spark, "eia_price").select(F.col("report_week").alias("date")),
    ]
    if spark.catalog.tableExists(f"{SILVER}.ais_zone_positions"):
        dates.append(silver(spark, "ais_zone_positions").select(F.to_date("event_ts").alias("date")))
    return dates


@dp.materialized_view(
    name="dim_date",
    schema=DIM_DATE_DDL,
    comment="One calendar day from the earliest fact date to a year ahead, plus Unknown / Not applicable / Not yet",
)
def dim_date_table():
    end = datetime.now(UTC).date() + timedelta(days=365)
    return dim_date(spark, calendar_start(fact_dates()), end)
