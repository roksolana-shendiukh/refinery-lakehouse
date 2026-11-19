from datetime import date

import pytest
from gold_testing import same_columns
from pyspark.sql import functions as F

from refinery.gold.dimensions import (
    PRICE_TYPES,
    VALUE_STATUSES,
    calendar_start,
    date_sk,
    dim_date,
    dim_status,
    status_sk,
)
from refinery.gold.keys import SPECIAL_MEMBERS
from refinery.gold.schemas import DIM_DATE_DDL, DIM_STATUS_DDL


def test_dim_status_matches_its_ddl(spark):
    assert same_columns(dim_status(spark), spark, DIM_STATUS_DDL)


def test_dim_status_has_every_combination_once_plus_special_members(spark):
    rows = dim_status(spark).collect()
    real = sorted((r.price_type, r.value_status) for r in rows if r.status_sk >= 0 or r.status_sk < -3)

    assert real == sorted((t, v) for t in PRICE_TYPES for v in VALUE_STATUSES)
    assert len(rows) == 9 + len(SPECIAL_MEMBERS)


def test_dim_status_key_is_the_one_facts_compute(spark):
    dim = dim_status(spark).where(~F.col("status_sk").between(-3, -1))
    expected = dim.select(status_sk(F.col("price_type"), F.col("value_status")).alias("expected"), "status_sk")

    assert expected.where(F.col("expected") != F.col("status_sk")).count() == 0


def test_dim_status_keys_are_unique_and_never_special(spark):
    keys = [r.status_sk for r in dim_status(spark).collect()]

    assert len(keys) == len(set(keys))


def test_dim_date_matches_its_ddl(spark):
    assert same_columns(dim_date(spark, date(2026, 1, 1), date(2026, 1, 3)), spark, DIM_DATE_DDL)


def test_dim_date_has_every_day_from_start_to_end_inclusive(spark):
    dim = dim_date(spark, date(2024, 2, 27), date(2024, 3, 2))
    days = sorted(r.date for r in dim.collect() if r.date is not None)

    assert days == [date(2024, 2, 27), date(2024, 2, 28), date(2024, 2, 29), date(2024, 3, 1), date(2024, 3, 2)]


def test_dim_date_attributes_of_a_day(spark):
    rows = {r.date_sk: r for r in dim_date(spark, date(2024, 2, 29), date(2024, 2, 29)).collect()}
    day = rows[20240229]

    assert (day.date, day.month_number, day.month_name, day.quarter, day.year) == (
        date(2024, 2, 29),
        2,
        "February",
        1,
        2024,
    )


def test_dim_date_special_members_have_no_date(spark):
    rows = {r.date_sk: r for r in dim_date(spark, date(2026, 1, 1), date(2026, 1, 1)).collect()}

    assert rows[-1].date is None
    assert rows[-1].month_name == "Unknown"


def test_date_sk_is_yyyymmdd(spark):
    df = spark.createDataFrame([(date(2026, 11, 14),)], "d DATE")

    assert df.select(date_sk(F.col("d"))).first()[0] == 20261114


def test_calendar_start_takes_the_earliest_and_skips_nulls_and_empty_tables(spark):
    supply = spark.createDataFrame([(date(2010, 5, 7),), (None,)], "date DATE")
    price = spark.createDataFrame([(date(1993, 4, 2),)], "date DATE")
    empty = spark.createDataFrame([], "date DATE")

    assert calendar_start([supply, price, empty]) == date(1993, 4, 2)


def test_calendar_start_fails_when_there_are_no_dates(spark):
    with pytest.raises(ValueError, match="No fact dates"):
        calendar_start([spark.createDataFrame([], "date DATE")])
