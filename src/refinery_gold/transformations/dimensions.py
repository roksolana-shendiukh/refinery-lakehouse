"""Gold dimensions as materialized views; the logic lives in refinery.gold.dimensions."""

from datetime import UTC, datetime, timedelta

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from refinery.gold.config import silver
from refinery.gold.dimensions import (
    dim_area,
    dim_date,
    dim_fuel_grade,
    dim_hub,
    dim_product,
    dim_status,
)
from refinery.gold.schemas import (
    DIM_AREA_DDL,
    DIM_DATE_DDL,
    DIM_FUEL_GRADE_DDL,
    DIM_HUB_DDL,
    DIM_PRODUCT_DDL,
    DIM_STATUS_DDL,
)


@dp.materialized_view(
    name="dim_status",
    schema=DIM_STATUS_DDL,
    comment="Junk dimension: every price type with every value status, plus Unknown / Not applicable / Not yet",
)
def dim_status_table():
    return dim_status(spark)


def fact_dates():
    """Dates the EIA facts use; the calendar starts at the earliest EIA week (STM)."""
    return [
        silver(spark, "eia_supply").select(F.col("report_week").alias("date")),
        silver(spark, "eia_price").select(F.col("report_week").alias("date")),
    ]


@dp.materialized_view(
    name="dim_date",
    schema=DIM_DATE_DDL,
    comment="One calendar day from the earliest EIA week to a year ahead, plus Unknown / Not applicable / Not yet",
)
def dim_date_table():
    end = datetime.now(UTC).date() + timedelta(days=365)
    return dim_date(fact_dates(), end)


@dp.materialized_view(
    name="dim_hub",
    schema=DIM_HUB_DDL,
    comment="One wholesale trading hub where spot prices are set, plus Unknown / Not applicable / Not yet",
)
def dim_hub_table():
    return dim_hub(silver(spark, "ref_hub"))


@dp.materialized_view(
    name="dim_product",
    schema=DIM_PRODUCT_DDL,
    comment="One refined petroleum product supplied on the U.S. market, plus Unknown / Not applicable / Not yet",
)
def dim_product_table():
    return dim_product(silver(spark, "ref_product"))


@dp.materialized_view(
    name="dim_fuel_grade",
    schema=DIM_FUEL_GRADE_DDL,
    comment="One priced grade of fuel or crude oil, plus Unknown / Not applicable / Not yet",
)
def dim_fuel_grade_table():
    return dim_fuel_grade(silver(spark, "ref_fuel_grade"))


@dp.materialized_view(
    name="dim_area",
    schema=DIM_AREA_DDL,
    comment="One U.S. fuel market (country, PADD region or subregion) with its hierarchy flattened, "
    "plus Unknown / Not applicable / Not yet",
)
def dim_area_table():
    return dim_area(silver(spark, "ref_area"))
