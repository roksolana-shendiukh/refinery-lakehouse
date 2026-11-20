from datetime import date

import pytest
from gold_testing import same_columns
from pyspark.sql import functions as F

from refinery.gold.dimensions import (
    PRICE_TYPES,
    VALUE_STATUSES,
    date_sk,
    dim_area,
    dim_date,
    dim_fuel_grade,
    dim_hub,
    dim_product,
    dim_status,
    from_reference,
    status_sk,
)
from refinery.gold.keys import SPECIAL_MEMBERS, UNKNOWN, hashed_sk
from refinery.gold.schemas import (
    DIM_AREA_DDL,
    DIM_DATE_DDL,
    DIM_FUEL_GRADE_DDL,
    DIM_HUB_DDL,
    DIM_PRODUCT_DDL,
    DIM_STATUS_DDL,
)


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


def dates(spark, *days):
    return spark.createDataFrame([(d,) for d in days], "date DATE")


def test_dim_date_matches_its_ddl(spark):
    assert same_columns(dim_date([dates(spark, date(2026, 1, 1))], date(2026, 1, 3)), spark, DIM_DATE_DDL)


def test_dim_date_has_every_day_from_the_earliest_date_to_end_inclusive(spark):
    dim = dim_date([dates(spark, date(2024, 3, 1), date(2024, 2, 27))], date(2024, 3, 2))
    days = sorted(r.date for r in dim.collect() if r.date is not None)

    assert days == [date(2024, 2, 27), date(2024, 2, 28), date(2024, 2, 29), date(2024, 3, 1), date(2024, 3, 2)]


def test_dim_date_starts_at_the_earliest_date_across_tables_skipping_nulls_and_empty_ones(spark):
    supply = dates(spark, date(2010, 5, 7), None)
    price = dates(spark, date(2010, 5, 5))
    empty = dates(spark)
    dim = dim_date([supply, price, empty], date(2010, 5, 7))

    assert min(r.date for r in dim.collect() if r.date is not None) == date(2010, 5, 5)


def test_dim_date_attributes_of_a_day(spark):
    rows = {r.date_sk: r for r in dim_date([dates(spark, date(2024, 2, 29))], date(2024, 2, 29)).collect()}
    day = rows[20240229]

    assert (day.date, day.month_number, day.month_name, day.quarter, day.year) == (
        date(2024, 2, 29),
        2,
        "February",
        1,
        2024,
    )


def test_dim_date_special_members_have_no_date(spark):
    rows = {r.date_sk: r for r in dim_date([dates(spark, date(2026, 1, 1))], date(2026, 1, 1)).collect()}

    assert rows[-1].date is None
    assert rows[-1].month_name == "Unknown"


def test_dim_date_builds_the_plan_without_running_and_fails_only_when_run(spark):
    calendar = dim_date([dates(spark)], date(2026, 1, 1))

    with pytest.raises(Exception, match="No fact dates"):
        calendar.collect()


def test_date_sk_is_yyyymmdd(spark):
    df = spark.createDataFrame([(date(2026, 11, 14),)], "d DATE")

    assert df.select(date_sk(F.col("d"))).first()[0] == 20261114


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


AREA_COLUMNS = "area_bk STRING, area_name STRING, area_level STRING, parent_area_bk STRING, is_coastal BOOLEAN"


def ref_area(spark):
    return spark.createDataFrame(
        [
            ("NUS", "U.S.", "country", None, True, None, None),
            ("R10", "PADD 1 (East Coast)", "region", "NUS", True, None, None),
            ("R1X", "PADD 1A (New England)", "subregion", "R10", True, None, None),
            ("R20", "PADD 2 (Midwest)", "region", "NUS", False, None, None),
            ("R9X", "Orphan subregion", "subregion", "R90", False, None, None),
        ],
        f"{AREA_COLUMNS}, {SILVER_METADATA}",
    )


def areas_by_bk(spark):
    return {r.area_bk: r for r in dim_area(ref_area(spark)).collect()}


def test_dim_area_matches_its_ddl(spark):
    assert same_columns(dim_area(ref_area(spark)), spark, DIM_AREA_DDL)


def test_dim_area_flattens_the_hierarchy_on_every_level(spark):
    areas = areas_by_bk(spark)

    def names(bk):
        return (areas[bk].country_name, areas[bk].region_name, areas[bk].subregion_name)

    assert names("NUS") == ("U.S.", None, None)
    assert names("R10") == ("U.S.", "PADD 1 (East Coast)", None)
    assert names("R1X") == ("U.S.", "PADD 1 (East Coast)", "PADD 1A (New England)")


def test_dim_area_parent_key_points_to_the_parent_row(spark):
    areas = areas_by_bk(spark)

    assert areas["NUS"].parent_area_sk is None
    assert areas["R10"].parent_area_sk == areas["NUS"].area_sk
    assert areas["R1X"].parent_area_sk == areas["R10"].area_sk


def test_dim_area_orphan_keeps_its_row_with_unknown_parent(spark):
    orphan = areas_by_bk(spark)["R9X"]

    assert orphan.parent_area_sk == UNKNOWN
    assert orphan.subregion_name == "Orphan subregion"
    assert orphan.region_name is None


def test_dim_area_keeps_every_area_plus_special_members(spark):
    areas = areas_by_bk(spark)

    assert set(areas) == {"NUS", "R10", "R1X", "R20", "R9X", "Unknown", "Not applicable", "Not yet"}
    assert areas["R20"].is_coastal is False
