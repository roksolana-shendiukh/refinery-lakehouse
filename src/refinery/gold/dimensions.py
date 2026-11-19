"""Gold dimensions: one function per dimension, each returns the rows of the table with its special members."""

from datetime import date
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from refinery.gold.keys import hashed_sk, with_special_members

PRICE_TYPES = ("retail", "spot", "n.a.")
VALUE_STATUSES = ("ok", "withheld", "missing")


def status_sk(price_type: Column, value_status: Column) -> Column:
    """The dim_status key; facts call this too, so both sides hash the same way."""
    return F.xxhash64(price_type, value_status)


def date_sk(date: Column) -> Column:
    """The dim_date key, yyyyMMdd as a number (20261114); facts call this too."""
    return F.date_format(date, "yyyyMMdd").cast("int")


def calendar_start(dates: list[DataFrame]) -> date:
    """Earliest date across one-column DataFrames named `date`; NULLs and empty tables are skipped."""
    earliest = reduce(DataFrame.union, dates).agg(F.min("date")).first()[0]
    if earliest is None:
        raise ValueError("No fact dates in silver yet: load EIA before building dim_date")
    return earliest


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


def dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """One row per day from start to end (inclusive), plus the special members."""
    days = spark.range(1).select(F.explode(F.sequence(F.lit(start), F.lit(end))).alias("date"))
    dim = days.select(
        date_sk(F.col("date")).alias("date_sk"),
        "date",
        F.month("date").cast("tinyint").alias("month_number"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.quarter("date").cast("tinyint").alias("quarter"),
        F.year("date").cast("smallint").alias("year"),
    )
    return with_special_members(dim, "date_sk")


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
