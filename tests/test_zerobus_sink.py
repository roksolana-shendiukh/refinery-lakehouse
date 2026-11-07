from datetime import UTC, datetime

from ais_producer.ack_callback import ProducerAckCallback
from ais_producer.ais_source import AisEnvelope
from ais_producer.config import ProducerConfig
from ais_producer.record_mapper import to_record
from ais_producer.zerobus_sink import ZerobusSink

CONFIG = ProducerConfig(
    zerobus_endpoint="https://endpoint",
    workspace_url="https://workspace",
    table_name="cat.bronze.zerobus_ais_messages",
    client_id="id",
    client_secret="secret",
    ais_api_key="key",
)


class FakeStream:
    def __init__(self) -> None:
        self.payloads: list[bytes] = []
        self.closed = False

    def ingest_record_offset(self, payload: bytes) -> int:
        self.payloads.append(payload)
        return len(self.payloads) - 1

    def close(self) -> None:
        self.closed = True


class FakeSdk:
    def __init__(self, endpoint: str, workspace_url: str) -> None:
        self.streams: list[FakeStream] = []

    def create_stream(self, client_id, client_secret, table, options) -> FakeStream:
        self.streams.append(FakeStream())
        return self.streams[-1]

    def recreate_stream(self, old: FakeStream) -> FakeStream:
        return self.create_stream(None, None, None, None)


def make_sink() -> tuple[ZerobusSink, FakeSdk]:
    sdks: list[FakeSdk] = []

    def factory(endpoint: str, workspace_url: str) -> FakeSdk:
        sdks.append(FakeSdk(endpoint, workspace_url))
        return sdks[-1]

    return ZerobusSink(CONFIG, ProducerAckCallback(), sdk_factory=factory), sdks[0]


def test_send_queues_serialized_records():
    sink, sdk = make_sink()
    sink.open()
    record = to_record(AisEnvelope(datetime(2026, 11, 8, tzinfo=UTC), "PositionReport", "{}"))

    offset = sink.send(record)

    assert offset == 0
    assert sdk.streams[0].payloads == [record.SerializeToString()]


def test_recreate_switches_to_a_new_stream_and_sets_the_flag():
    sink, sdk = make_sink()
    sink.open()

    sink.recreate()

    assert len(sdk.streams) == 2
    assert sink.recreated
    sink.recovered()
    assert not sink.recreated


def test_close_closes_the_stream_once():
    sink, sdk = make_sink()
    sink.open()

    sink.close()
    sink.close()

    assert sdk.streams[0].closed
