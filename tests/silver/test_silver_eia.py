from datetime import date, datetime
from decimal import Decimal

from silver_testing import finding_names, with_metadata

from refinery.silver.eia import SUPPLY_CHECKS, to_supply
from refinery.silver.quality import check

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


SILVER_SUPPLY = "report_week DATE, area_bk STRING, product_bk STRING, rate_kbd DECIMAL(10,1), value_status STRING"
FRIDAY = date(2026, 11, 6)
CLEAN = (FRIDAY, "NUS", "EPM0F", Decimal("9000.5"), "ok")


def supply_findings(spark, engine, row):
    df = with_metadata(spark.createDataFrame([row], SILVER_SUPPLY))
    lookups = {
        "ref_area": spark.createDataFrame([("NUS",)], "area_bk STRING"),
        "ref_product": spark.createDataFrame([("EPM0F",)], "product_bk STRING"),
    }
    return finding_names(check(df, SUPPLY_CHECKS, engine, lookups))


def test_supply_check_levels_follow_the_stm():
    levels = {c["name"]: c["criticality"] for c in SUPPLY_CHECKS}

    assert levels == {
        "eia_supply.report_week.not_null": "error",
        "eia_supply.report_week.friday": "error",
        "eia_supply.area_bk.exists": "warn",
        "eia_supply.product_bk.exists": "warn",
        "eia_supply.rate_kbd.status_matches_value": "error",
        "eia_supply.rate_kbd.not_negative": "error",
        "eia_supply.value_status.ok": "warn",
        "eia_supply.loaded_at.not_null": "error",
        "eia_supply.source_file.not_null": "warn",
    }


def test_clean_supply_row_has_no_findings(spark, engine):
    assert supply_findings(spark, engine, CLEAN) == []


def test_report_week_must_be_a_friday(spark, engine):
    saturday = (date(2026, 11, 7), "NUS", "EPM0F", Decimal("1.0"), "ok")

    assert supply_findings(spark, engine, saturday) == ["eia_supply.report_week.friday"]


def test_missing_report_week_is_an_error(spark, engine):
    row = (None, "NUS", "EPM0F", Decimal("1.0"), "ok")

    assert supply_findings(spark, engine, row) == ["eia_supply.report_week.not_null"]


def test_unknown_area_and_product_are_warnings(spark, engine):
    row = (FRIDAY, "XXX", "NOPE", Decimal("1.0"), "ok")

    assert supply_findings(spark, engine, row) == ["eia_supply.area_bk.exists", "eia_supply.product_bk.exists"]


def test_withheld_value_is_only_a_warning(spark, engine):
    row = (FRIDAY, "NUS", "EPM0F", None, "withheld")

    assert supply_findings(spark, engine, row) == ["eia_supply.value_status.ok"]


def test_rate_must_agree_with_value_status(spark, engine):
    row = (FRIDAY, "NUS", "EPM0F", None, "ok")

    assert supply_findings(spark, engine, row) == ["eia_supply.rate_kbd.status_matches_value"]


def test_negative_rate_is_an_error(spark, engine):
    row = (FRIDAY, "NUS", "EPM0F", Decimal("-1.0"), "ok")

    assert supply_findings(spark, engine, row) == ["eia_supply.rate_kbd.not_negative"]
