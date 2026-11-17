from datetime import date, datetime
from decimal import Decimal

from silver_testing import finding_names, with_metadata

from refinery.silver.eia import SUPPLY_CHECKS, to_price, to_supply
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


BRONZE_PRICE = (
    "route STRING, period STRING, duoarea STRING, product STRING, series STRING, value STRING, units STRING, "
    "source_file STRING, ingested_at TIMESTAMP"
)


def price_row(route="pri/gnd", period="2026-11-09", duoarea="SCA", product="EPMR", value="3.456", units="$/GAL"):
    return (route, period, duoarea, product, " S1 ", value, units, "file", datetime(2026, 11, 14))


def price(spark, *rows):
    products = spark.createDataFrame(
        [("EPMR", "retail", "REGULAR"), ("EPMRR", "spot", "REGULAR")],
        "eia_product STRING, price_type STRING, grade_bk STRING",
    )
    hubs = spark.createDataFrame([("Y35NY", "R10")], "hub_bk STRING, served_area_bk STRING")
    return to_price(spark.createDataFrame(list(rows), BRONZE_PRICE), products, hubs)


def test_price_has_the_stm_columns_then_source_units(spark):
    assert price(spark, price_row()).columns == [
        "report_week",
        "price_type",
        "area_bk",
        "hub_bk",
        "grade_bk",
        "price_usd_gal",
        "value_status",
        "series_id",
        "loaded_at",
        "source_file",
        "source_units",
    ]


def test_retail_price_moves_monday_to_friday_and_keeps_the_area(spark):
    row = price(spark, price_row(period="2026-11-09", duoarea=" sca ")).first()

    assert (row.report_week, row.price_type, row.area_bk, row.hub_bk) == (date(2026, 11, 6), "retail", "SCA", None)
    assert (row.grade_bk, row.price_usd_gal, row.value_status, row.series_id) == (
        "REGULAR",
        Decimal("3.456"),
        "ok",
        "S1",
    )


def test_spot_price_takes_the_hub_and_its_served_area(spark):
    row = price(spark, price_row(route="pri/spt", period="2026-11-06", duoarea="y35ny", product="EPMRR")).first()

    assert (row.report_week, row.price_type, row.hub_bk, row.area_bk, row.grade_bk) == (
        date(2026, 11, 6),
        "spot",
        "Y35NY",
        "R10",
        "REGULAR",
    )


def test_barrel_price_is_divided_by_42(spark):
    row = price(spark, price_row(route="pri/spt", duoarea="Y35NY", product="EPMRR", value="105", units="$/BBL")).first()

    assert row.price_usd_gal == Decimal("2.500")


def test_price_value_status_and_unknown_units(spark):
    rows = price(
        spark,
        price_row(duoarea="A", value="W"),
        price_row(duoarea="B", value=None),
        price_row(duoarea="C", units="$/MCF"),
    ).collect()

    result = {r.area_bk: (r.price_usd_gal, r.value_status, r.source_units) for r in rows}

    assert result == {
        "A": (None, "withheld", "$/GAL"),
        "B": (None, "missing", "$/GAL"),
        "C": (None, "ok", "$/MCF"),
    }


def test_unmatched_product_and_hub_keep_the_row_with_nulls(spark):
    row = price(spark, price_row(route="pri/spt", duoarea="NOHUB", product="NOPE")).first()

    assert (row.hub_bk, row.area_bk, row.grade_bk) == ("NOHUB", None, None)
