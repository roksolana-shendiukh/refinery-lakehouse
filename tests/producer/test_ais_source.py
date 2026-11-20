import asyncio
from datetime import UTC, datetime

import pytest
from websockets.asyncio.server import serve

from ais_producer.ais_source import AisSourceError, SourceStats, parse, read_messages, subscription

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


def run_against(handler, silence_seconds: int, wanted: int, stats: SourceStats | None = None) -> list[str]:
    """Runs read_messages against a local websocket server until `wanted` messages arrived."""

    async def scenario():
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            received = []
            url = f"ws://127.0.0.1:{port}"
            messages = read_messages(url, "key", [], TYPES, 20, 20, silence_seconds, stats)
            async for envelope in messages:
                received.append(envelope.raw)
                if len(received) == wanted:
                    break
            await messages.aclose()
            return received

    return asyncio.run(asyncio.wait_for(scenario(), timeout=10))


def test_a_silent_open_connection_is_closed_and_opened_again():
    connections = []

    async def handler(websocket):
        connections.append(await websocket.recv())  # the subscription
        await websocket.send(POSITION)
        if len(connections) == 1:
            await websocket.wait_closed()  # first connection: open, answers pings, sends nothing more

    stats = SourceStats()

    assert run_against(handler, silence_seconds=1, wanted=2, stats=stats) == [POSITION, POSITION]
    assert len(connections) == 2
    assert (stats.connections, stats.silences, stats.disconnects, stats.received) == (2, 1, 0, 2)


def test_a_steady_feed_keeps_its_connection():
    connections = []

    async def handler(websocket):
        connections.append(await websocket.recv())
        for _ in range(4):
            await websocket.send(POSITION)
            await asyncio.sleep(0.4)  # well inside the 1 s silence limit
        await websocket.wait_closed()

    stats = SourceStats()

    assert len(run_against(handler, silence_seconds=1, wanted=4, stats=stats)) == 4
    assert len(connections) == 1
    assert (stats.connections, stats.silences, stats.disconnects, stats.received) == (1, 0, 0, 4)
    assert stats.seconds_since_last_message() < 5


def test_a_dropped_connection_is_opened_again_and_counted():
    connections = []

    async def handler(websocket):
        connections.append(await websocket.recv())
        await websocket.send(POSITION)
        if len(connections) == 1:
            await websocket.close(code=1011, reason="server restart")  # aisstream drops the first connection
        else:
            await websocket.wait_closed()

    stats = SourceStats()

    assert run_against(handler, silence_seconds=60, wanted=2, stats=stats) == [POSITION, POSITION]
    assert (stats.connections, stats.silences, stats.disconnects, stats.received) == (2, 0, 1, 2)


def test_no_message_yet_has_no_age():
    assert SourceStats().seconds_since_last_message() is None
