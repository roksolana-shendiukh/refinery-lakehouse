from datetime import UTC, datetime

import pytest
from gold_testing import same_columns
from pyspark.sql import functions as F

from refinery.gold.dimensions import dim_vessel, size_band
from refinery.gold.schemas import DIM_VESSEL_DDL

VESSEL_MAP_COLUMNS = "imo INT, mmsi INT, effective_from TIMESTAMP, effective_to TIMESTAMP, first_static_ts TIMESTAMP, loaded_at TIMESTAMP"
STATIC_COLUMNS = (
    "mmsi INT, imo INT, vessel_name STRING, ship_type SMALLINT, length_m SMALLINT, draught DECIMAL(4,1), "
    "__START_AT TIMESTAMP"
)


def at(day):
    """Midnight UTC of a day in November 2026."""
    return datetime(2026, 11, day, tzinfo=UTC)


def fleet(spark):
    """Silver inputs for six vessels, one per case the STM describes."""
    vessel_map = spark.createDataFrame(
        [
            (9321483, 244000000, at(1), at(10), at(1), None),
            (9321483, 366111111, at(10), None, at(10), None),
            (9111111, 538000001, at(2), None, at(2), None),
            (9222222, 538000001, at(15), None, at(15), None),
        ],
        VESSEL_MAP_COLUMNS,
    )
    static = spark.createDataFrame(
        [
            (244000000, 9321483, "NORDIC STAR", 80, 250, None, at(1)),
            (366111111, 9321483, "NORDIC STAR II", None, 250, None, at(10)),
            (538000001, 9111111, "TANKER C", 81, 230, None, at(2)),
            (538000001, 9222222, "TANKER D", 83, 270, None, at(15)),
            (367000002, None, "SMALL TANKER", 82, 90, None, at(5)),
            (211000004, None, "HARBOUR TUG", 52, 30, None, at(4)),
        ],
        STATIC_COLUMNS,
    )
    positions = spark.createDataFrame(
        [(367000002, at(3)), (636000003, at(7)), (211000004, at(4))],
        "mmsi INT, event_ts TIMESTAMP",
    )
    ref_mid = spark.createDataFrame(
        [
            ("244", "Netherlands"),
            ("366", "United States"),
            ("367", "United States"),
            ("538", "Marshall Islands"),
            ("636", "Liberia"),
            ("211", "Germany"),
        ],
        "mid STRING, country_name STRING",
    )
    return vessel_map, static, positions, ref_mid


def vessels_by_version(spark):
    """Real versions keyed by (mmsi, start day); timestamps formatted in Spark, so in UTC on every machine."""
    dim = dim_vessel(*fleet(spark)).where(~F.col("vessel_sk").between(-3, -1))
    days = [F.date_format(c, "MM-dd").alias(f"{c}_day") for c in ("effective_from", "effective_to", "inferred_at")]
    return {(r.mmsi, r.effective_from_day): r for r in dim.select("*", *days).collect()}


def test_dim_vessel_matches_its_ddl(spark):
    assert same_columns(dim_vessel(*fleet(spark)), spark, DIM_VESSEL_DDL)


def test_dim_vessel_keeps_tankers_and_inferred_vessels_only(spark):
    assert sorted(vessels_by_version(spark)) == sorted(
        [
            (244000000, "11-01"),
            (366111111, "11-10"),
            (538000001, "11-02"),
            (538000001, "11-15"),
            (367000002, "11-03"),
            (636000003, "11-07"),
        ]
    )


def test_dim_vessel_new_mmsi_starts_a_new_version_with_its_own_flag(spark):
    versions = vessels_by_version(spark)
    old, new = versions[(244000000, "11-01")], versions[(366111111, "11-10")]

    assert (old.vessel_bk, old.flag_country, old.effective_to_day, old.is_current) == (
        9321483,
        "Netherlands",
        "11-10",
        False,
    )
    assert (new.vessel_bk, new.flag_country, new.effective_to_day, new.is_current) == (
        9321483,
        "United States",
        None,
        True,
    )


def test_dim_vessel_type1_attributes_are_the_latest_non_empty_on_every_version(spark):
    versions = vessels_by_version(spark)

    for key in [(244000000, "11-01"), (366111111, "11-10")]:
        vessel = versions[key]
        assert (vessel.vessel_name, vessel.ship_type, vessel.length_m, vessel.size_band) == (
            "NORDIC STAR II",
            80,
            250,
            "250–300 m",
        )


def test_dim_vessel_reused_mmsi_closes_the_earlier_vessel_version(spark):
    versions = vessels_by_version(spark)
    first, second = versions[(538000001, "11-02")], versions[(538000001, "11-15")]

    assert (first.vessel_bk, first.effective_to_day, first.is_current) == (9111111, "11-15", True)
    assert (second.vessel_bk, second.effective_to_day, second.is_current) == (9222222, None, True)


def test_dim_vessel_mmsi_only_vessel_starts_at_its_first_sighting(spark):
    vessel = vessels_by_version(spark)[(367000002, "11-03")]

    assert (vessel.vessel_bk, vessel.vessel_name, vessel.size_band) == (None, "SMALL TANKER", "<150 m")
    assert (vessel.is_inferred, vessel.inferred_at_day, vessel.is_current) == (False, "11-03", True)


def test_dim_vessel_inferred_vessel_is_a_placeholder_until_its_static_data_arrives(spark):
    vessel = vessels_by_version(spark)[(636000003, "11-07")]

    assert (vessel.vessel_name, vessel.ship_type, vessel.size_band, vessel.flag_country) == (
        "Unknown",
        None,
        "Unknown",
        "Liberia",
    )
    assert (vessel.is_inferred, vessel.inferred_at_day, vessel.is_current) == (True, "11-07", True)


def test_dim_vessel_key_is_the_hash_of_mmsi_and_start_and_unique(spark):
    dim = dim_vessel(*fleet(spark)).where(~F.col("vessel_sk").between(-3, -1))

    assert dim.where(F.xxhash64("mmsi", "effective_from") != F.col("vessel_sk")).count() == 0
    assert dim.select("vessel_sk").distinct().count() == dim.count()


@pytest.mark.parametrize(
    "length, band",
    [(149, "<150 m"), (150, "150–200 m"), (299, "250–300 m"), (300, ">300 m"), (None, "Unknown")],
)
def test_size_band_boundaries(spark, length, band):
    df = spark.createDataFrame([(length,)], "length_m SMALLINT")

    assert df.select(size_band(F.col("length_m"))).first()[0] == band
