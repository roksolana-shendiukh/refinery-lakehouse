from datetime import datetime, timezone

import pytest

from ais_producer.ais_source import AisEnvelope
from ais_producer.record_mapper import to_epoch_micros, to_record

RAW = '{"MessageType": "PositionReport", "MetaData": {"MMSI": 538001234}, "Message": {}}'


def test_epoch_micros_is_exact():
    moment = datetime(2026, 11, 7, 12, 0, 0, 123456, tzinfo=timezone.utc)

    assert to_epoch_micros(moment) == 1_794_052_800_123_456


def test_naive_time_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        to_epoch_micros(datetime(2026, 11, 7, 12, 0))


def test_record_keeps_the_message_unchanged():
    envelope = AisEnvelope(
        received_at=datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        message_type="PositionReport",
        raw=RAW,
    )

    record = to_record(envelope)

    assert record.received_at == 1_000_000
    assert record.message_type == "PositionReport"
    assert record.raw_message == RAW


def test_record_round_trips_through_protobuf_bytes():
    envelope = AisEnvelope(datetime(2026, 11, 7, tzinfo=timezone.utc), "ShipStaticData", RAW)
    record = to_record(envelope)

    restored = type(record).FromString(record.SerializeToString())

    assert restored == record
