"""Converts an AIS envelope into the Protobuf record of the landing table. Pure functions, no network."""

from datetime import UTC, datetime, timedelta

from ais_producer.ais_message_pb2 import AisMessage
from ais_producer.ais_source import AisEnvelope

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
ONE_MICROSECOND = timedelta(microseconds=1)


def to_epoch_micros(moment: datetime) -> int:
    """TIMESTAMP columns travel as int64 microseconds since 1970-01-01 UTC."""
    if moment.tzinfo is None:
        raise ValueError("received_at must be timezone-aware (UTC)")
    return (moment - EPOCH) // ONE_MICROSECOND


def to_record(envelope: AisEnvelope) -> AisMessage:
    """One landing-table row: receive time, message type and the unchanged AIS JSON (VARIANT as a JSON string)."""
    return AisMessage(
        received_at=to_epoch_micros(envelope.received_at),
        message_type=envelope.message_type,
        raw_message=envelope.raw,
    )
