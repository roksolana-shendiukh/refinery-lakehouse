"""Gold dimensions: one function per dimension, each returns the rows of the table with its special members."""

from datetime import date
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from refinery.gold.keys import UNKNOWN, hashed_sk, with_special_members

PRICE_TYPES = ("retail", "spot", "n.a.")
VALUE_STATUSES = ("ok", "withheld", "missing")


def status_sk(price_type: Column, value_status: Column) -> Column:
    """The dim_status key; facts call this too, so both sides hash the same way."""
    return F.xxhash64(price_type, value_status)


def date_sk(date: Column) -> Column:
    """The dim_date key, yyyyMMdd as a number (20261114); facts call this too."""
    return F.date_format(date, "yyyyMMdd").cast("int")


MINUTES_PER_DAY = 24 * 60


def time_sk(ts: Column) -> Column:
    """The dim_time key, hhmm as a number (835 for 08:35); facts call this too."""
    return (F.hour(ts) * 100 + F.minute(ts)).cast("int")


def from_reference(ref: DataFrame, sk_col: str, bk_col: str, attributes: list[str]) -> DataFrame:
    """A Type 1 dimension from a silver reference table: hashed key, business key, attributes, special members."""
    dim = ref.select(hashed_sk(F.col(bk_col)).alias(sk_col), bk_col, *attributes)
    return with_special_members(dim, sk_col)


def dim_status(spark: SparkSession) -> DataFrame:
    combinations = spark.createDataFrame(
        [(price_type, value_status) for price_type in PRICE_TYPES for value_status in VALUE_STATUSES],
        "price_type STRING, value_status STRING",
    )
    dim = combinations.select(
        status_sk(F.col("price_type"), F.col("value_status")).alias("status_sk"),
        "price_type",
        "value_status",
    )
    return with_special_members(dim, "status_sk")


NO_FACT_DATES = "No fact dates in silver yet: load EIA before building dim_date"


def dim_date(fact_dates: list[DataFrame], end: date) -> DataFrame:
    """One row per day from the earliest fact date to end (inclusive), plus the special members."""
    bounds = reduce(DataFrame.unionByName, fact_dates).agg(F.min("date").alias("start"))
    start = F.when(F.col("start").isNull(), F.raise_error(F.lit(NO_FACT_DATES))).otherwise(F.col("start"))
    days = bounds.select(F.explode(F.sequence(start, F.lit(end))).alias("date"))
    dim = days.select(
        date_sk(F.col("date")).alias("date_sk"),
        "date",
        F.month("date").cast("tinyint").alias("month_number"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.quarter("date").cast("tinyint").alias("quarter"),
        F.year("date").cast("smallint").alias("year"),
    )
    return with_special_members(dim, "date_sk")


def dim_time(spark: SparkSession) -> DataFrame:
    """One row per minute of the day (1,440): the time of day facts happen at, plus the special members."""
    minutes = spark.range(MINUTES_PER_DAY).select(
        F.floor(F.col("id") / 60).cast("int").alias("hour"),
        (F.col("id") % 60).cast("int").alias("minute"),
    )
    hour = F.col("hour")
    dim = minutes.select(
        (hour * 100 + F.col("minute")).cast("int").alias("time_sk"),
        F.format_string("%02d:%02d", hour, F.col("minute")).alias("time"),
        hour.cast("tinyint").alias("hour"),
        F.col("minute").cast("tinyint").alias("minute"),
        F.when(hour < 6, "night")
        .when(hour < 12, "morning")
        .when(hour < 18, "afternoon")
        .otherwise("evening")
        .alias("day_part"),
    )
    return with_special_members(dim, "time_sk")


def dim_hub(ref_hub: DataFrame) -> DataFrame:
    """One wholesale trading hub; served_area_bk is NULL for ZEU (Europe), which serves no U.S. area."""
    return from_reference(ref_hub, "hub_sk", "hub_bk", ["hub_name", "served_area_bk"])


def dim_product(ref_product: DataFrame) -> DataFrame:
    """One refined product; representative_grade_bk links gasoline and diesel to the grade that prices them."""
    return from_reference(
        ref_product,
        "product_sk",
        "product_bk",
        ["product_name", "product_family", "unit", "representative_grade_bk"],
    )


def dim_fuel_grade(ref_fuel_grade: DataFrame) -> DataFrame:
    """One priced grade of fuel or crude; the price in the fact is always $/gal, source_unit is descriptive."""
    return from_reference(ref_fuel_grade, "grade_sk", "grade_bk", ["grade_name", "source_unit"])


def dim_port(ref_terminal_zone: DataFrame) -> DataFrame:
    """One terminal zone where tankers load, unload or wait; boundary is the zone polygon as WKT text."""
    zones = ref_terminal_zone.withColumnRenamed("boundary_wkt", "boundary")
    return from_reference(zones, "port_sk", "port_bk", ["port_name", "terminal_type", "area_bk", "boundary"])


def dim_area(ref_area: DataFrame) -> DataFrame:
    """One U.S. fuel market with its hierarchy flattened: country, region and subregion names on every row."""
    area = ref_area.select("area_bk", "area_name", "area_level", "parent_area_bk", "is_coastal")
    parent = area.select(
        F.col("area_bk").alias("p_bk"),
        F.col("area_name").alias("p_name"),
        F.col("parent_area_bk").alias("p_parent"),
    )
    grandparent = area.select(F.col("area_bk").alias("g_bk"), F.col("area_name").alias("g_name"))

    tree = area.join(F.broadcast(parent), F.col("parent_area_bk") == F.col("p_bk"), "left").join(
        F.broadcast(grandparent), F.col("p_parent") == F.col("g_bk"), "left"
    )

    level = F.col("area_level")
    dim = tree.select(
        hashed_sk(F.col("area_bk")).alias("area_sk"),
        "area_bk",
        "area_name",
        "area_level",
        F.when(F.col("parent_area_bk").isNull(), F.lit(None).cast("bigint"))
        .when(F.col("p_bk").isNull(), F.lit(UNKNOWN).cast("bigint"))
        .otherwise(hashed_sk(F.col("parent_area_bk")))
        .alias("parent_area_sk"),
        F.when(level == "country", F.col("area_name"))
        .when(level == "region", F.col("p_name"))
        .otherwise(F.col("g_name"))
        .alias("country_name"),
        F.when(level == "region", F.col("area_name")).when(level == "subregion", F.col("p_name")).alias("region_name"),
        F.when(level == "subregion", F.col("area_name")).alias("subregion_name"),
        "is_coastal",
    )
    return with_special_members(dim, "area_sk")
