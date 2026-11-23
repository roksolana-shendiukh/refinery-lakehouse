from datetime import UTC, datetime
from decimal import Decimal

from gold_testing import same_columns
from pyspark.sql import functions as F

from refinery.gold.facts import fact_port_call, port_calls
from refinery.gold.keys import NOT_APPLICABLE, NOT_YET, UNKNOWN
from refinery.gold.schemas import FACT_PORT_CALL_DDL

TANKER, TUG = 244000000, 211000004


def at(day, hour, minute=0):
    """A moment in November 2026, UTC."""
    return datetime(2026, 11, day, hour, minute, tzinfo=UTC)


NOW = at(12, 12)


def positions(spark):
    return spark.createDataFrame(
        [
            (TANKER, "HOU", at(2, 8), Decimal("10.0"), 0),
            (TANKER, "HOU", at(2, 12), Decimal("0.2"), 0),
            (TANKER, "HOU", at(2, 17), Decimal("0.1"), 5),
            (TANKER, "HOU", at(3, 1), Decimal("8.0"), 0),
            (TANKER, "HOU", at(3, 2), Decimal("9.0"), 0),
            (TANKER, "XXX", at(5, 10), Decimal("10.0"), 0),
            (TANKER, "HOU", at(12, 10), Decimal("1.0"), 1),
            (TUG, "HOU", at(2, 9), Decimal("0.0"), 5),
        ],
        "mmsi INT, port_bk STRING, event_ts TIMESTAMP, sog DECIMAL(4,1), nav_status SMALLINT",
    )


def dim_vessel(spark):
    return spark.createDataFrame(
        [(1, TANKER, at(1, 0), at(10, 0)), (2, TANKER, at(10, 0), None), (UNKNOWN, None, None, None)],
        "vessel_sk BIGINT, mmsi INT, effective_from TIMESTAMP, effective_to TIMESTAMP",
    )


def static(spark):
    return spark.createDataFrame(
        [
            (TANKER, Decimal("9.5"), at(1, 0)),
            (TANKER, Decimal("12.0"), at(2, 8, 30)),
            (TANKER, None, at(6, 0)),
            (TANKER, Decimal("7.0"), at(11, 0)),
        ],
        "mmsi INT, draught DECIMAL(4,1), __START_AT TIMESTAMP",
    )


def ref_terminal_zone(spark):
    return spark.createDataFrame([("HOU", "R30")], "port_bk STRING, area_bk STRING")


def ref_area(spark):
    return spark.createDataFrame([("R30",)], "area_bk STRING")


def calls_by_entry(spark):
    """port_calls keyed by entry time; times formatted in Spark, so in UTC on every machine."""
    calls = port_calls(positions(spark), dim_vessel(spark), NOW)
    formatted = calls.select(
        "mmsi",
        F.date_format("entered_ts", "MM-dd HH:mm").alias("entered"),
        F.date_format("moored_ts", "MM-dd HH:mm").alias("moored"),
        F.date_format("departed_ts", "MM-dd HH:mm").alias("departed"),
    )
    return {r.entered: r for r in formatted.collect()}


def fact_by_entry(spark):
    fact = fact_port_call(
        positions(spark), dim_vessel(spark), static(spark), ref_terminal_zone(spark), ref_area(spark), NOW
    )
    return {(r.entered_date_sk, r.entered_time_sk): r for r in fact.collect()}


# --- port_calls: gaps and islands ---


def test_port_calls_start_a_new_call_after_a_gap_over_6_hours_only(spark):
    assert sorted(calls_by_entry(spark)) == ["11-02 08:00", "11-03 01:00", "11-05 10:00", "11-12 10:00"]


def test_port_calls_keep_tankers_only(spark):
    assert {r.mmsi for r in calls_by_entry(spark).values()} == {TANKER}


def test_port_call_moored_at_its_first_stationary_point_and_departed_after_6_silent_hours(spark):
    calls = calls_by_entry(spark)

    assert (calls["11-02 08:00"].moored, calls["11-02 08:00"].departed) == ("11-02 12:00", "11-02 17:00")
    assert (calls["11-03 01:00"].moored, calls["11-03 01:00"].departed) == (None, "11-03 02:00")
    assert (calls["11-12 10:00"].moored, calls["11-12 10:00"].departed) == (None, None)


# --- fact_port_call ---


def test_fact_port_call_matches_its_ddl(spark):
    fact = fact_port_call(
        positions(spark), dim_vessel(spark), static(spark), ref_terminal_zone(spark), ref_area(spark), NOW
    )

    assert same_columns(fact, spark, FACT_PORT_CALL_DDL)


def test_fact_port_call_uses_the_vessel_version_valid_at_entry(spark):
    fact = fact_by_entry(spark)

    assert fact[(20261102, 800)].vessel_sk == 1
    assert fact[(20261112, 1000)].vessel_sk == 2


def test_fact_port_call_moored_and_departed_keys_say_why_they_are_missing(spark):
    fact = fact_by_entry(spark)
    berthed, left_unberthed, waiting = fact[(20261102, 800)], fact[(20261103, 100)], fact[(20261112, 1000)]

    assert (berthed.moored_date_sk, berthed.moored_time_sk) == (20261102, 1200)
    assert (berthed.departed_date_sk, berthed.departed_time_sk) == (20261102, 1700)
    assert (left_unberthed.moored_date_sk, left_unberthed.moored_time_sk) == (NOT_APPLICABLE, NOT_APPLICABLE)
    assert (waiting.moored_date_sk, waiting.departed_date_sk, waiting.departed_time_sk) == (NOT_YET, NOT_YET, NOT_YET)


def test_fact_port_call_wait_and_berth_hours(spark):
    fact = fact_by_entry(spark)

    assert (fact[(20261102, 800)].wait_hours, fact[(20261102, 800)].berth_hours) == (Decimal("4.00"), Decimal("5.00"))
    assert (fact[(20261103, 100)].wait_hours, fact[(20261103, 100)].berth_hours) == (Decimal("1.00"), None)
    assert (fact[(20261112, 1000)].wait_hours, fact[(20261112, 1000)].berth_hours) == (None, None)


def test_fact_port_call_unknown_zone_gets_unknown_port_and_area(spark):
    fact = fact_by_entry(spark)

    assert (fact[(20261105, 1000)].port_sk, fact[(20261105, 1000)].area_sk) == (UNKNOWN, UNKNOWN)
    assert fact[(20261102, 800)].area_sk != UNKNOWN


def test_fact_port_call_arrival_draught_is_the_latest_reported_up_to_an_hour_after_entry(spark):
    fact = fact_by_entry(spark)

    assert fact[(20261102, 800)].arrival_draught == Decimal("12.0")
    assert fact[(20261112, 1000)].arrival_draught == Decimal("7.0")
