import json
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from silver_testing import finding_names

from refinery.silver.ais import (
    MMSI_MAP_CHECKS,
    POSITION_CHECKS,
    STATIC_CHECKS,
    positions_in_zones,
    to_positions,
    to_static,
    to_vessel_mmsi_map,
)
from refinery.silver.quality import check

TIME_UTC = "2026-11-08 14:27:52.331783091 +0000 UTC"
RECEIVED_AT = datetime(2026, 11, 8, 14, 27, 53)
EVENT_TS = datetime(2026, 11, 8, 14, 30)
LOADED_AT = datetime(2026, 11, 8, 14, 31)


def position_message(mmsi=244000000, time_utc=TIME_UTC, lat=51.9, lon=4.1, sog=11.2, nav_status=0):
    return json.dumps(
        {
            "Message": {
                "PositionReport": {
                    "Latitude": lat,
                    "Longitude": lon,
                    "Sog": sog,
                    "NavigationalStatus": nav_status,
                    "Cog": 10.0,
                    "TrueHeading": 11,
                }
            },
            "MessageType": "PositionReport",
            "MetaData": {"MMSI": mmsi, "time_utc": time_utc},
        }
    )


def static_message(
    mmsi=244000000,
    time_utc=TIME_UTC,
    imo=9321483,
    name="NORDIC STAR",
    ship_type=80,
    to_bow=120,
    to_stern=60,
    draught=12.34,
):
    return json.dumps(
        {
            "Message": {
                "ShipStaticData": {
                    "ImoNumber": imo,
                    "Name": name,
                    "Type": ship_type,
                    "MaximumStaticDraught": draught,
                    "Dimension": {"A": to_bow, "B": to_stern, "C": 10, "D": 10},
                }
            },
            "MessageType": "ShipStaticData",
            "MetaData": {"MMSI": mmsi, "time_utc": time_utc},
        }
    )


def landing(spark, *messages):
    """Rows of the landing table: (message_type, raw JSON text), parsed into a VARIANT column."""
    rows = [(message_type, raw, RECEIVED_AT) for message_type, raw in messages]
    df = spark.createDataFrame(rows, "message_type STRING, raw_message STRING, received_at TIMESTAMP")
    return df.withColumn("raw_message", F.parse_json("raw_message"))


def position(spark, **fields):
    return to_positions(landing(spark, ("PositionReport", position_message(**fields)))).first()


def static(spark, **fields):
    return to_static(landing(spark, ("ShipStaticData", static_message(**fields)))).first()


# --- ais_zone_positions: transformation ---


def test_positions_have_the_stm_columns_and_a_position(spark):
    positions = to_positions(landing(spark, ("PositionReport", position_message())))

    assert positions.columns == ["mmsi", "event_ts", "latitude", "longitude", "sog", "nav_status", "loaded_at"]


def test_position_values_and_event_time(spark):
    positions = to_positions(landing(spark, ("PositionReport", position_message())))
    row = positions.select(
        "mmsi",
        "latitude",
        "longitude",
        F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("event_ts"),
        "sog",
        "nav_status",
    ).first()

    assert (row.mmsi, row.latitude, row.longitude, row.event_ts) == (244000000, 51.9, 4.1, "2026-11-08 14:27:52.331783")
    assert (str(row.sog), row.nav_status) == ("11.2", 0)


@pytest.mark.parametrize(
    ("time_utc", "expected"),
    [
        ("2026-11-22 18:09:39.46628 +0000 UTC", "2026-11-22 18:09:39.466280"),
        ("2026-11-08 14:27:13.375 +0000 UTC", "2026-11-08 14:27:13.375000"),
        ("2026-11-08 14:27:13 +0000 UTC", "2026-11-08 14:27:13.000000"),
        ("2026-11-08 14:27:52.3317830 +0000 UTC", "2026-11-08 14:27:52.331783"),
    ],
)
def test_event_time_with_any_number_of_fraction_digits(spark, time_utc, expected):
    """The producer drops trailing zeros, so the fraction has from none to nine digits."""
    positions = to_positions(landing(spark, ("PositionReport", position_message(time_utc=time_utc))))
    row = positions.select(F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("event_ts")).first()

    assert row.event_ts == expected


def test_only_position_reports_are_kept(spark):
    messages = landing(
        spark,
        ("PositionReport", position_message()),
        ("ShipStaticData", static_message(mmsi=244000001)),
    )

    assert to_positions(messages).count() == 1


def test_unparsable_time_becomes_null(spark):
    assert position(spark, time_utc="not a time").event_ts is None


def test_not_available_speed_and_status_become_null(spark):
    row = position(spark, sog=102.3, nav_status=15)

    assert (row.sog, row.nav_status) == (None, None)


def test_missing_mmsi_becomes_null(spark):
    assert position(spark, mmsi=None).mmsi is None


# --- ais_vessel_static: transformation ---


def test_static_has_the_stm_columns(spark):
    columns = to_static(landing(spark, ("ShipStaticData", static_message()))).columns

    assert columns == ["mmsi", "event_ts", "imo", "vessel_name", "ship_type", "length_m", "draught", "loaded_at"]


def test_static_values(spark):
    row = static(spark, name="  NORDIC STAR ")

    assert (row.mmsi, row.imo, row.vessel_name, row.ship_type, row.length_m) == (
        244000000,
        9321483,
        "NORDIC STAR",
        80,
        180,
    )
    assert str(row.draught) == "12.3"


def test_only_static_messages_are_kept_in_static(spark):
    messages = landing(spark, ("ShipStaticData", static_message()), ("PositionReport", position_message()))

    assert to_static(messages).count() == 1


def test_not_available_static_values_become_null(spark):
    row = static(spark, imo=0, name="   ", ship_type=0, to_bow=0, to_stern=0, draught=0)

    assert (row.imo, row.vessel_name, row.ship_type, row.length_m, row.draught) == (None, None, None, None, None)


def zone_join_inputs(spark):
    """A position and a terminal zone with the columns of the real ref_terminal_zone, loaded_at included."""
    positions = spark.createDataFrame(
        [(244000000, EVENT_TS, 51.9, 4.1, Decimal("11.2"), 0, LOADED_AT)],
        "mmsi int, event_ts timestamp, latitude double, longitude double, sog decimal(4,1), nav_status smallint,"
        " loaded_at timestamp",
    )
    zones = spark.createDataFrame(
        [("NLRTM", "Rotterdam", "POLYGON ((0 0, 1 0, 1 1, 0 0))", datetime(2026, 11, 1), "reference.csv")],
        "port_bk string, port_name string, boundary_wkt string, loaded_at timestamp, source_file string",
    )
    return positions, zones


def test_position_in_zone_keeps_its_own_loaded_at(spark):
    positions, zones = zone_join_inputs(spark)
    result = positions_in_zones(positions, zones, F.col("latitude") > 0)
    assert result.columns == ["mmsi", "event_ts", "port_bk", "sog", "nav_status", "loaded_at"]
    row = result.first()
    assert row.port_bk == "NLRTM"
    assert row.loaded_at == LOADED_AT


def test_position_outside_every_zone_is_dropped(spark):
    positions, zones = zone_join_inputs(spark)
    assert positions_in_zones(positions, zones, F.col("latitude") < 0).count() == 0


# --- ais_zone_positions: checks ---

SILVER_POSITION = (
    "mmsi INT, event_ts TIMESTAMP, port_bk STRING, sog DECIMAL(4,1), nav_status SMALLINT, loaded_at TIMESTAMP"
)


def silver_position(**changes):
    """A clean position row, with some columns changed."""
    row = {
        "mmsi": 244000000,
        "event_ts": EVENT_TS,
        "port_bk": "RTM",
        "sog": Decimal("11.2"),
        "nav_status": 0,
        "loaded_at": LOADED_AT,
        **changes,
    }
    return tuple(row.values())


def position_findings(spark, engine, row):
    df = spark.createDataFrame([row], SILVER_POSITION)
    return finding_names(check(df, POSITION_CHECKS, engine))


def test_position_check_levels_follow_the_stm():
    levels = {c["name"]: c["criticality"] for c in POSITION_CHECKS}

    assert levels == {
        "ais_zone_positions.mmsi.not_null": "error",
        "ais_zone_positions.mmsi.nine_digits": "error",
        "ais_zone_positions.event_ts.not_null": "error",
        "ais_zone_positions.event_ts.not_in_future": "error",
        "ais_zone_positions.sog.in_range": "warn",
        "ais_zone_positions.nav_status.in_range": "warn",
        "ais_zone_positions.loaded_at.not_null": "error",
    }


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        pytest.param(silver_position(), [], id="clean"),
        pytest.param(silver_position(mmsi=None), ["ais_zone_positions.mmsi.not_null"], id="no mmsi"),
        pytest.param(silver_position(mmsi=12345), ["ais_zone_positions.mmsi.nine_digits"], id="short mmsi"),
        pytest.param(silver_position(event_ts=None), ["ais_zone_positions.event_ts.not_null"], id="no event time"),
        pytest.param(
            silver_position(event_ts=LOADED_AT + timedelta(minutes=6)),
            ["ais_zone_positions.event_ts.not_in_future"],
            id="event time in the future",
        ),
        pytest.param(silver_position(event_ts=LOADED_AT + timedelta(minutes=4)), [], id="small clock skew"),
        pytest.param(silver_position(sog=Decimal("-1.0")), ["ais_zone_positions.sog.in_range"], id="negative speed"),
        pytest.param(silver_position(sog=None, nav_status=None), [], id="missing speed and status"),
        pytest.param(silver_position(nav_status=20), ["ais_zone_positions.nav_status.in_range"], id="bad status"),
        pytest.param(silver_position(loaded_at=None), ["ais_zone_positions.loaded_at.not_null"], id="no loaded_at"),
    ],
)
def test_position_row_findings(spark, engine, row, expected):
    assert position_findings(spark, engine, row) == expected


# --- ais_vessel_static: checks ---

SILVER_STATIC = (
    "mmsi INT, event_ts TIMESTAMP, imo INT, vessel_name STRING, ship_type SMALLINT, length_m SMALLINT, "
    "draught DECIMAL(4,1), loaded_at TIMESTAMP"
)


def silver_static(**changes):
    """A clean static row, with some columns changed."""
    row = {
        "mmsi": 244000000,
        "event_ts": EVENT_TS,
        "imo": 9321483,
        "vessel_name": "NORDIC STAR",
        "ship_type": 80,
        "length_m": 180,
        "draught": Decimal("12.3"),
        "loaded_at": LOADED_AT,
        **changes,
    }
    return tuple(row.values())


def static_findings(spark, engine, row):
    df = spark.createDataFrame([row], SILVER_STATIC)
    return finding_names(check(df, STATIC_CHECKS, engine))


def test_static_check_levels_follow_the_stm():
    levels = {c["name"]: c["criticality"] for c in STATIC_CHECKS}

    assert levels == {
        "ais_vessel_static.mmsi.not_null": "error",
        "ais_vessel_static.mmsi.nine_digits": "error",
        "ais_vessel_static.event_ts.not_null": "error",
        "ais_vessel_static.event_ts.not_in_future": "error",
        "ais_vessel_static.imo.in_range": "warn",
        "ais_vessel_static.vessel_name.not_null": "warn",
        "ais_vessel_static.ship_type.in_range": "warn",
        "ais_vessel_static.length_m.in_range": "warn",
        "ais_vessel_static.draught.in_range": "warn",
        "ais_vessel_static.loaded_at.not_null": "error",
    }


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        pytest.param(silver_static(), [], id="clean"),
        pytest.param(silver_static(mmsi=None), ["ais_vessel_static.mmsi.not_null"], id="no mmsi"),
        pytest.param(silver_static(mmsi=12345), ["ais_vessel_static.mmsi.nine_digits"], id="short mmsi"),
        pytest.param(silver_static(event_ts=None), ["ais_vessel_static.event_ts.not_null"], id="no event time"),
        pytest.param(
            silver_static(event_ts=LOADED_AT + timedelta(minutes=6)),
            ["ais_vessel_static.event_ts.not_in_future"],
            id="event time in the future",
        ),
        pytest.param(silver_static(imo=123), ["ais_vessel_static.imo.in_range"], id="bad imo"),
        pytest.param(silver_static(imo=None), [], id="no imo"),
        pytest.param(silver_static(vessel_name=None), ["ais_vessel_static.vessel_name.not_null"], id="no name"),
        pytest.param(silver_static(ship_type=100), ["ais_vessel_static.ship_type.in_range"], id="bad ship type"),
        pytest.param(silver_static(length_m=600), ["ais_vessel_static.length_m.in_range"], id="too long"),
        pytest.param(silver_static(draught=Decimal("40.0")), ["ais_vessel_static.draught.in_range"], id="too deep"),
        pytest.param(silver_static(loaded_at=None), ["ais_vessel_static.loaded_at.not_null"], id="no loaded_at"),
    ],
)
def test_static_row_findings(spark, engine, row, expected):
    assert static_findings(spark, engine, row) == expected


# --- vessel_mmsi_map: transformation ---

STATIC_SILVER_FOR_MAP = "mmsi INT, event_ts TIMESTAMP, imo INT, loaded_at TIMESTAMP"
POSITIONS_SILVER_FOR_MAP = "mmsi INT, event_ts TIMESTAMP"


def vessel_map(spark, static_rows, position_rows=()):
    static_df = spark.createDataFrame(static_rows, STATIC_SILVER_FOR_MAP)
    positions_df = spark.createDataFrame(list(position_rows), POSITIONS_SILVER_FOR_MAP)
    return to_vessel_mmsi_map(static_df, positions_df)


def test_vessel_map_has_the_stm_columns(spark):
    columns = vessel_map(spark, [(244000000, datetime(2026, 11, 1, 10), 9321483, datetime(2026, 11, 1, 11))]).columns

    assert columns == ["imo", "mmsi", "effective_from", "effective_to", "first_static_ts", "loaded_at"]


def test_vessel_map_pair_uses_first_static_and_last_load(spark):
    static_rows = [
        (244000000, datetime(2026, 11, 2, 10), 9321483, datetime(2026, 11, 2, 10, 1)),
        (244000000, datetime(2026, 11, 3, 10), 9321483, datetime(2026, 11, 3, 10, 1)),
    ]
    row = vessel_map(spark, static_rows, [(244000000, datetime(2026, 11, 1, 8))]).first()

    assert row.first_static_ts == datetime(2026, 11, 2, 10)
    assert row.effective_from == datetime(2026, 11, 1, 8)
    assert row.loaded_at == datetime(2026, 11, 3, 10, 1)
    assert row.effective_to is None


def test_vessel_map_without_positions_starts_at_first_static(spark):
    row = vessel_map(spark, [(244000000, datetime(2026, 11, 2, 10), 9321483, datetime(2026, 11, 2, 11))]).first()

    assert row.effective_from == datetime(2026, 11, 2, 10)


def test_vessel_map_closes_a_pair_when_the_vessel_gets_a_new_mmsi(spark):
    static_rows = [
        (244000000, datetime(2026, 11, 1, 10), 9321483, datetime(2026, 11, 1, 11)),
        (244000001, datetime(2026, 11, 5, 10), 9321483, datetime(2026, 11, 5, 11)),
    ]
    rows = vessel_map(spark, static_rows).orderBy("effective_from").collect()

    assert [(r.mmsi, r.effective_to) for r in rows] == [(244000000, datetime(2026, 11, 5, 10)), (244000001, None)]


def test_vessel_map_ignores_static_without_imo(spark):
    static_rows = [(244000000, datetime(2026, 11, 1, 10), None, datetime(2026, 11, 1, 11))]

    assert vessel_map(spark, static_rows).count() == 0


# --- vessel_mmsi_map: checks ---

SILVER_MAP = (
    "imo INT, mmsi INT, effective_from TIMESTAMP, effective_to TIMESTAMP, first_static_ts TIMESTAMP, "
    "loaded_at TIMESTAMP"
)


def silver_map(**changes):
    """A clean map row (the only row of its vessel), with some columns changed."""
    row = {
        "imo": 9321483,
        "mmsi": 244000000,
        "effective_from": EVENT_TS,
        "effective_to": None,
        "first_static_ts": EVENT_TS,
        "loaded_at": LOADED_AT,
        **changes,
    }
    return tuple(row.values())


def map_findings(spark, engine, *rows):
    df = spark.createDataFrame(list(rows), SILVER_MAP)
    return sorted(finding_names(check(df, MMSI_MAP_CHECKS, engine)))


def test_vessel_map_check_levels_follow_the_stm():
    levels = {c["name"]: c["criticality"] for c in MMSI_MAP_CHECKS}

    assert levels == {
        "vessel_mmsi_map.imo.in_range": "warn",
        "vessel_mmsi_map.mmsi.not_null": "error",
        "vessel_mmsi_map.mmsi.nine_digits": "error",
        "vessel_mmsi_map.effective_from.not_null": "error",
        "vessel_mmsi_map.effective_from.unique": "error",
        "vessel_mmsi_map.effective_from.not_after_first_static": "error",
        "vessel_mmsi_map.effective_to.after_effective_from": "error",
        "vessel_mmsi_map.effective_to.one_open_per_vessel": "error",
        "vessel_mmsi_map.first_static_ts.not_null": "error",
        "vessel_mmsi_map.loaded_at.not_null": "error",
    }


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        pytest.param(silver_map(), [], id="clean"),
        pytest.param(silver_map(imo=123), ["vessel_mmsi_map.imo.in_range"], id="bad imo"),
        pytest.param(silver_map(mmsi=None), ["vessel_mmsi_map.mmsi.not_null"], id="no mmsi"),
        pytest.param(silver_map(mmsi=12345), ["vessel_mmsi_map.mmsi.nine_digits"], id="short mmsi"),
        pytest.param(
            silver_map(effective_from=None),
            ["vessel_mmsi_map.effective_from.not_null"],
            id="no effective_from",
        ),
        pytest.param(
            silver_map(effective_from=EVENT_TS + timedelta(hours=1)),
            ["vessel_mmsi_map.effective_from.not_after_first_static"],
            id="starts after first static",
        ),
        pytest.param(
            silver_map(first_static_ts=None),
            ["vessel_mmsi_map.first_static_ts.not_null"],
            id="no first_static_ts",
        ),
        pytest.param(silver_map(loaded_at=None), ["vessel_mmsi_map.loaded_at.not_null"], id="no loaded_at"),
    ],
)
def test_vessel_map_row_findings(spark, engine, row, expected):
    assert map_findings(spark, engine, row) == expected


def test_vessel_map_closed_pair_followed_by_an_open_one_is_clean(spark, engine):
    closed = silver_map(mmsi=244000000, effective_to=EVENT_TS + timedelta(days=1))
    current = silver_map(
        mmsi=244000001,
        effective_from=EVENT_TS + timedelta(days=1),
        first_static_ts=EVENT_TS + timedelta(days=1),
    )

    assert map_findings(spark, engine, closed, current) == []


def test_vessel_map_vessel_with_two_open_rows_is_flagged(spark, engine):
    first = silver_map(mmsi=244000000)
    second = silver_map(mmsi=244000001)

    assert map_findings(spark, engine, first, second) == ["vessel_mmsi_map.effective_to.one_open_per_vessel"] * 2


def test_vessel_map_vessel_without_an_open_row_is_flagged(spark, engine):
    row = silver_map(effective_to=EVENT_TS + timedelta(days=1))

    assert map_findings(spark, engine, row) == ["vessel_mmsi_map.effective_to.one_open_per_vessel"]


def test_vessel_map_effective_to_not_after_effective_from_is_flagged(spark, engine):
    closed = silver_map(mmsi=244000000, effective_to=EVENT_TS)
    current = silver_map(mmsi=244000001)

    assert map_findings(spark, engine, closed, current) == ["vessel_mmsi_map.effective_to.after_effective_from"]


def test_vessel_map_repeated_mmsi_and_effective_from_is_flagged(spark, engine):
    first = silver_map(imo=9321483)
    second = silver_map(imo=9074729)

    assert map_findings(spark, engine, first, second) == ["vessel_mmsi_map.effective_from.unique"] * 2
