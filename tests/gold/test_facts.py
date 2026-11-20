from datetime import date, datetime
from decimal import Decimal

from gold_testing import same_columns
from pyspark.sql import functions as F

from refinery.gold.dimensions import dim_area, dim_product, dim_status
from refinery.gold.facts import fact_supply
from refinery.gold.keys import UNKNOWN
from refinery.gold.schemas import FACT_SUPPLY_DDL

SUPPLY_COLUMNS = (
    "report_week DATE, area_bk STRING, product_bk STRING, rate_kbd DECIMAL(10,1), value_status STRING, "
    "loaded_at TIMESTAMP, source_file STRING"
)
WEEK = date(2026, 11, 6)
FIRST_LOAD, REVISION = datetime(2026, 11, 12), datetime(2026, 11, 19)


def supply(spark, rows):
    return spark.createDataFrame(rows, SUPPLY_COLUMNS)


def ref_area(spark):
    return spark.createDataFrame(
        [("NUS", "U.S.", "country", None, True)],
        "area_bk STRING, area_name STRING, area_level STRING, parent_area_bk STRING, is_coastal BOOLEAN",
    )


def ref_product(spark):
    return spark.createDataFrame(
        [("EPM0F", "Finished motor gasoline", "gasoline", "MBBL/D", "GASOLINE")],
        "product_bk STRING, product_name STRING, product_family STRING, unit STRING, representative_grade_bk STRING",
    )


def build(spark, rows):
    return fact_supply(supply(spark, rows), ref_area(spark), ref_product(spark))


def test_fact_supply_matches_its_ddl(spark):
    fact = build(spark, [(WEEK, "NUS", "EPM0F", Decimal("8850.0"), "ok", FIRST_LOAD, "f1")])

    assert same_columns(fact, spark, FACT_SUPPLY_DDL)


def test_fact_supply_keeps_one_row_with_the_latest_revision(spark):
    fact = build(
        spark,
        [
            (WEEK, "NUS", "EPM0F", Decimal("8912.0"), "ok", REVISION, "f2"),
            (WEEK, "NUS", "EPM0F", Decimal("8850.0"), "ok", FIRST_LOAD, "f1"),
        ],
    )
    rows = fact.collect()

    assert len(rows) == 1
    assert rows[0].rate_kbd == Decimal("8912.0")


def test_fact_supply_takes_value_and_status_from_the_same_revision(spark):
    fact = build(
        spark,
        [
            (WEEK, "NUS", "EPM0F", Decimal("8850.0"), "ok", FIRST_LOAD, "f1"),
            (WEEK, "NUS", "EPM0F", None, "withheld", REVISION, "f2"),
        ],
    )
    statuses = {
        r.status_sk: r.code
        for r in dim_status(spark)
        .select("status_sk", F.concat_ws("/", "price_type", "value_status").alias("code"))
        .collect()
    }
    row = fact.first()

    assert row.rate_kbd is None
    assert statuses[row.status_sk] == "n.a./withheld"


def test_fact_supply_gives_unknown_to_an_area_or_product_missing_from_the_references(spark):
    fact = build(spark, [(WEEK, "R99", "XXXX", Decimal("100.0"), "ok", FIRST_LOAD, "f1")])
    row = fact.first()

    assert (row.area_sk, row.product_sk) == (UNKNOWN, UNKNOWN)
    assert row.rate_kbd == Decimal("100.0")


def test_fact_supply_keys_are_in_the_dimensions(spark):
    fact = build(spark, [(WEEK, "NUS", "EPM0F", Decimal("8850.0"), "ok", FIRST_LOAD, "f1")])
    row = fact.first()
    areas = {r.area_bk: r.area_sk for r in dim_area(ref_area(spark)).collect()}
    products = {r.product_bk: r.product_sk for r in dim_product(ref_product(spark)).collect()}

    assert row.date_sk == 20261106
    assert row.area_sk == areas["NUS"]
    assert row.product_sk == products["EPM0F"]
