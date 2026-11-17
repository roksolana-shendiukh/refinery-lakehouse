import json
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from silver_testing import finding_names

from refinery.silver.ais import POSITION_CHECKS, STATIC_CHECKS, to_positions, to_static
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
