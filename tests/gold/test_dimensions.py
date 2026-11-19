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
    dim_fuel_grade,
    dim_hub,
    dim_product,
    dim_status,
    from_reference,
    status_sk,
)
from refinery.gold.keys import SPECIAL_MEMBERS, hashed_sk
from refinery.gold.schemas import DIM_DATE_DDL, DIM_FUEL_GRADE_DDL, DIM_HUB_DDL, DIM_PRODUCT_DDL, DIM_STATUS_DDL


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


SILVER_METADATA = "loaded_at TIMESTAMP, source_file STRING"


def ref_hub(spark):
    return spark.createDataFrame(
        [("NYH", "New York Harbor", "R10", None, None), ("ZEU", "Europe", None, None, None)],
        f"hub_bk STRING, hub_name STRING, served_area_bk STRING, {SILVER_METADATA}",
    )


def ref_product(spark):
    return spark.createDataFrame(
        [("EPM0F", "Finished motor gasoline", "gasoline", "MBBL/D", "GASOLINE", None, None)],
        "product_bk STRING, product_name STRING, product_family STRING, unit STRING, "
        f"representative_grade_bk STRING, {SILVER_METADATA}",
    )


def ref_fuel_grade(spark):
    return spark.createDataFrame(
        [("ULSD", "Ultra-low sulfur diesel", "$/GAL", "EPD0", None, None)],
        f"grade_bk STRING, grade_name STRING, source_unit STRING, product_bk STRING, {SILVER_METADATA}",
    )


@pytest.mark.parametrize(
    "build, ref, ddl",
    [
        (dim_hub, ref_hub, DIM_HUB_DDL),
        (dim_product, ref_product, DIM_PRODUCT_DDL),
        (dim_fuel_grade, ref_fuel_grade, DIM_FUEL_GRADE_DDL),
    ],
)
def test_reference_dimension_matches_its_ddl(spark, build, ref, ddl):
    assert same_columns(build(ref(spark)), spark, ddl)


def test_reference_dimension_key_is_the_hash_facts_compute(spark):
    dim = dim_hub(ref_hub(spark)).where(~F.col("hub_sk").between(-3, -1))
    mismatched = dim.where(hashed_sk(F.col("hub_bk")) != F.col("hub_sk"))

    assert mismatched.count() == 0


def test_reference_dimension_keeps_a_real_null(spark):
    rows = {r.hub_bk: r for r in dim_hub(ref_hub(spark)).collect()}

    assert rows["ZEU"].served_area_bk is None
    assert rows["NYH"].served_area_bk == "R10"


def test_reference_dimension_keeps_every_reference_row_plus_special_members(spark):
    dim = from_reference(ref_hub(spark), "hub_sk", "hub_bk", ["hub_name"])

    assert sorted(r.hub_bk for r in dim.collect()) == sorted(["NYH", "ZEU", "Unknown", "Not applicable", "Not yet"])
