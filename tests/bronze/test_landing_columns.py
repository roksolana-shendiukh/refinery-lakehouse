from datetime import datetime

import pytest

from refinery.bronze.landing_columns import to_bronze, to_reference_bronze

LANDING = "/Volumes/refinery_dev/dev_bronze/landing/eia"
LOAD = "load_id=20261113T173236Z/offset=0.json"

# What Auto Loader hands over: EIA field names as in the JSON, plus source_file and _rescued_data
RAW_SCHEMA = (
    "period STRING, duoarea STRING, `area-name` STRING, product STRING, `product-name` STRING, "
    "process STRING, series STRING, value STRING, units STRING, "
    "source_file STRING, _rescued_data STRING"
)

BRONZE_COLUMNS = [
    "route",
    "period",
    "duoarea",
    "area_name",
    "product",
    "product_name",
    "process",
    "series",
    "value",
    "units",
    "source_file",
    "ingested_at",
    "_rescued_data",
    "load_date",
]


def landed(spark, folder, value="8945"):
    return spark.createDataFrame(
        [
            (
                "2026-11-06",
                "NUS",
                "U.S.",
                "EPM0F",
                "Finished Motor Gasoline",
                "VPP",
                "WGFUPUS2",
                value,
                "MBBL/D",
                f"{LANDING}/{folder}/{LOAD}",
                None,
            )
        ],
        RAW_SCHEMA,
    )


def test_to_bronze_has_the_stm_columns_in_order(spark):
    assert to_bronze(landed(spark, "supply")).columns == BRONZE_COLUMNS


def test_to_bronze_renames_eia_fields_with_dashes(spark):
    row = to_bronze(landed(spark, "supply")).collect()[0]

    assert row["area_name"] == "U.S."
    assert row["product_name"] == "Finished Motor Gasoline"


def test_to_bronze_keeps_eia_value_exactly_as_text(spark):
    result = to_bronze(landed(spark, "supply", value="W"))

    assert dict(result.dtypes)["value"] == "string"
    assert result.collect()[0]["value"] == "W"


@pytest.mark.parametrize(
    ("folder", "route"),
    [("supply", "cons/wpsup"), ("price/retail", "pri/gnd"), ("price/spot", "pri/spt")],
)
def test_to_bronze_reads_route_from_landing_folder(spark, folder, route):
    assert to_bronze(landed(spark, folder)).collect()[0]["route"] == route


def test_to_bronze_load_date_is_the_day_of_ingested_at(spark):
    result = to_bronze(landed(spark, "supply"))

    assert isinstance(result.collect()[0]["ingested_at"], datetime)
    assert result.where("load_date = to_date(ingested_at)").count() == 1


def test_to_reference_bronze_keeps_csv_columns_then_metadata(spark):
    df = spark.createDataFrame(
        [("Y35NY", "New York Harbor", "R10", "/Volumes/x/reference/ref_hub/sha256=ab/ref_hub.csv", None)],
        "hub_bk STRING, hub_name STRING, served_area_bk STRING, source_file STRING, _rescued_data STRING",
    )

    result = to_reference_bronze(df, "ref_hub")

    assert result.columns == [
        "hub_bk",
        "hub_name",
        "served_area_bk",
        "source_file",
        "ingested_at",
        "_rescued_data",
        "load_date",
    ]
    assert result.collect()[0]["hub_name"] == "New York Harbor"
