from datetime import UTC, datetime

import pytest

from ais_producer.ais_source import AisSourceError, parse, subscription

TYPES = ("PositionReport", "ShipStaticData")
NOW = datetime(2026, 11, 7, 12, 0, tzinfo=UTC)
POSITION = '{"MessageType": "PositionReport", "MetaData": {"MMSI": 538001234}, "Message": {}}'


def test_position_report_is_kept_unchanged():
    envelope = parse(POSITION.encode("utf-8"), TYPES, NOW)

    assert envelope.message_type == "PositionReport"
    assert envelope.raw == POSITION
    assert envelope.received_at == NOW


def test_service_messages_are_skipped():
    assert parse('{"MessageType": "SubscriptionConfirmation"}', TYPES, NOW) is None


def test_broken_frames_are_skipped():
    assert parse("not json", TYPES, NOW) is None


def test_error_from_aisstream_stops_the_source():
    with pytest.raises(AisSourceError, match="Api Key"):
        parse('{"error": "Api Key Is Not Valid"}', TYPES, NOW)


def test_subscription_asks_only_for_needed_types():
    request = subscription("key", [((24.0, -98.0), (31.0, -82.0))], TYPES)

    assert '"FilterMessageTypes": ["PositionReport", "ShipStaticData"]' in request
    assert '"BoundingBoxes": [[[24.0, -98.0], [31.0, -82.0]]]' in request
