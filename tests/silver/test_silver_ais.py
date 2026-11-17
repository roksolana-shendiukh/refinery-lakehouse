import json
from datetime import datetime

from pyspark.sql import functions as F

from refinery.silver.ais import to_positions, to_static

TIME_UTC = "2026-11-08 14:27:52.331783091 +0000 UTC"
RECEIVED_AT = datetime(2026, 11, 8, 14, 27, 53)


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
