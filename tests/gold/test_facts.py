from datetime import date, datetime
from decimal import Decimal

from gold_testing import same_columns
from pyspark.sql import functions as F

from refinery.gold.dimensions import dim_area, dim_fuel_grade, dim_hub, dim_product, dim_status
from refinery.gold.facts import fact_price, fact_supply
from refinery.gold.keys import NOT_APPLICABLE, UNKNOWN
from refinery.gold.schemas import FACT_PRICE_DDL, FACT_SUPPLY_DDL

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


def ref_area_price(spark):
    return spark.createDataFrame(
        [("NUS", "U.S.", "country", None, True), ("R10", "PADD 1 (East Coast)", "region", "NUS", True)],
        "area_bk STRING, area_name STRING, area_level STRING, parent_area_bk STRING, is_coastal BOOLEAN",
    )


def ref_hub(spark):
    return spark.createDataFrame(
        [("NYH", "New York Harbor", "R10"), ("ZEU", "Europe", None)],
        "hub_bk STRING, hub_name STRING, served_area_bk STRING",
    )


def ref_fuel_grade(spark):
    return spark.createDataFrame(
        [
            ("GASOLINE", "Regular gasoline", "$/GAL", "EPM0F"),
            ("ULSD", "Ultra-low sulfur diesel", "$/GAL", "EPD0"),
            ("WTI", "WTI crude oil", "$/BBL", None),
        ],
        "grade_bk STRING, grade_name STRING, source_unit STRING, product_bk STRING",
    )


PRICE_COLUMNS = (
    "report_week DATE, price_type STRING, area_bk STRING, hub_bk STRING, grade_bk STRING, "
    "price_usd_gal DECIMAL(9,3), value_status STRING, series_id STRING, loaded_at TIMESTAMP, source_file STRING"
)


def prices(spark):
    def row(series, price_type, area, hub, grade):
        return (WEEK, price_type, area, hub, grade, Decimal("2.500"), "ok", series, FIRST_LOAD, "f1")

    return spark.createDataFrame(
        [
            row("retail_gasoline", "retail", "NUS", None, "GASOLINE"),
            row("spot_gasoline", "spot", "R10", "NYH", "GASOLINE"),
            row("spot_brent", "spot", None, "ZEU", "WTI"),
            row("spot_unknown_hub", "spot", "R10", "XXX", "GASOLINE"),
            row("retail_unknown_area", "retail", "R99", None, "GASOLINE"),
            row("retail_diesel", "retail", "NUS", None, "ULSD"),
            row("retail_unknown_grade", "retail", "NUS", None, "XXXX"),
        ],
        PRICE_COLUMNS,
    )


def price_rows(spark, eia_price=None):
    fact = fact_price(
        eia_price or prices(spark), ref_fuel_grade(spark), ref_area_price(spark), ref_hub(spark), ref_product(spark)
    )
    return {r.series_id: r for r in fact.collect()}


def test_fact_price_matches_its_ddl(spark):
    fact = fact_price(prices(spark), ref_fuel_grade(spark), ref_area_price(spark), ref_hub(spark), ref_product(spark))

    assert same_columns(fact, spark, FACT_PRICE_DDL)


def test_fact_price_keeps_every_price(spark):
    assert len(price_rows(spark)) == 7


def test_fact_price_retail_has_no_hub(spark):
    assert price_rows(spark)["retail_gasoline"].hub_sk == NOT_APPLICABLE


def test_fact_price_unknown_hub_is_unknown(spark):
    assert price_rows(spark)["spot_unknown_hub"].hub_sk == UNKNOWN


def test_fact_price_hub_serving_no_us_area_has_no_area(spark):
    assert price_rows(spark)["spot_brent"].area_sk == NOT_APPLICABLE


def test_fact_price_unknown_area_is_unknown(spark):
    assert price_rows(spark)["retail_unknown_area"].area_sk == UNKNOWN


def test_fact_price_crude_has_no_product(spark):
    assert price_rows(spark)["spot_brent"].product_sk == NOT_APPLICABLE


def test_fact_price_grade_whose_product_is_missing_is_unknown(spark):
    assert price_rows(spark)["retail_diesel"].product_sk == UNKNOWN


def test_fact_price_unknown_grade_has_unknown_grade_and_product(spark):
    row = price_rows(spark)["retail_unknown_grade"]

    assert (row.grade_sk, row.product_sk) == (UNKNOWN, UNKNOWN)


def test_fact_price_keys_are_in_the_dimensions(spark):
    row = price_rows(spark)["spot_gasoline"]
    hubs = {r.hub_bk: r.hub_sk for r in dim_hub(ref_hub(spark)).collect()}
    grades = {r.grade_bk: r.grade_sk for r in dim_fuel_grade(ref_fuel_grade(spark)).collect()}
    areas = {r.area_bk: r.area_sk for r in dim_area(ref_area_price(spark)).collect()}
    products = {r.product_bk: r.product_sk for r in dim_product(ref_product(spark)).collect()}

    assert (row.date_sk, row.hub_sk, row.grade_sk) == (20261106, hubs["NYH"], grades["GASOLINE"])
    assert (row.area_sk, row.product_sk) == (areas["R10"], products["EPM0F"])


def test_fact_price_status_carries_the_price_type(spark):
    statuses = {r.status_sk: (r.price_type, r.value_status) for r in dim_status(spark).collect()}
    rows = price_rows(spark)

    assert statuses[rows["retail_gasoline"].status_sk] == ("retail", "ok")
    assert statuses[rows["spot_gasoline"].status_sk] == ("spot", "ok")
    assert rows["retail_gasoline"].price == Decimal("2.500")


def test_fact_price_keeps_the_latest_revision_with_its_status_and_series(spark):
    revised = spark.createDataFrame(
        [
            (WEEK, "retail", "NUS", None, "GASOLINE", Decimal("3.100"), "ok", "old_series", FIRST_LOAD, "f1"),
            (WEEK, "retail", "NUS", None, "GASOLINE", None, "withheld", "new_series", REVISION, "f2"),
        ],
        PRICE_COLUMNS,
    )
    rows = price_rows(spark, revised)

    assert list(rows) == ["new_series"]
    assert rows["new_series"].price is None
