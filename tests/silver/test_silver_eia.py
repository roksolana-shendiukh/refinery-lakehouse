from datetime import date, datetime
from decimal import Decimal

from refinery.silver.eia import to_supply

BRONZE_SUPPLY = "period STRING, duoarea STRING, product STRING, value STRING, source_file STRING, ingested_at TIMESTAMP"


def bronze_row(period="2026-11-06", duoarea="NUS", product="EPM0F", value="9000.5"):
    return (period, duoarea, product, value, "file", datetime(2026, 11, 8))


def supply(spark, *rows):
    return to_supply(spark.createDataFrame(list(rows), BRONZE_SUPPLY))


def test_supply_has_the_stm_columns_and_types(spark):
    silver = supply(spark, bronze_row())

    assert silver.columns == [
        "report_week",
        "area_bk",
        "product_bk",
        "rate_kbd",
        "value_status",
        "loaded_at",
        "source_file",
    ]
    assert dict(silver.dtypes)["report_week"] == "date"
    assert dict(silver.dtypes)["rate_kbd"] == "decimal(10,1)"


def test_supply_week_and_area_are_cleaned(spark):
    row = supply(spark, bronze_row(period="2026-11-06", duoarea=" nus ")).first()

    assert (row.report_week, row.area_bk) == (date(2026, 11, 6), "NUS")


def test_supply_invalid_period_becomes_null(spark):
    assert supply(spark, bronze_row(period="2026-13-45")).first().report_week is None


def test_value_status_tells_ok_withheld_and_missing(spark):
    silver = supply(
        spark,
        bronze_row(product="a", value=" 1234.5 "),
        bronze_row(product="b", value="W"),
        bronze_row(product="c", value=" w "),
        bronze_row(product="d", value=None),
        bronze_row(product="e", value="n/a"),
    )

    result = {r.product_bk: (r.rate_kbd, r.value_status) for r in silver.collect()}

    assert result == {
        "a": (Decimal("1234.5"), "ok"),
        "b": (None, "withheld"),
        "c": (None, "withheld"),
        "d": (None, "missing"),
        "e": (None, "missing"),
    }
