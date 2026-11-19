import asyncio
import logging
from datetime import UTC, datetime

from ais_producer.ais_source import AisEnvelope
from ais_producer.buffer import MessageBuffer

AT = datetime(2026, 11, 8, tzinfo=UTC)


def position(n: int) -> AisEnvelope:
    return AisEnvelope(AT, "PositionReport", f"p{n}")


def static(n: int) -> AisEnvelope:
    return AisEnvelope(AT, "ShipStaticData", f"s{n}")


def drain(buffer: MessageBuffer) -> list[str]:
    async def everything():
        buffer.close()
        out = []
        while (item := await buffer.get()) is not None:
            out.append(item.raw)
        return out

    return asyncio.run(everything())


def test_first_in_first_out():
    buffer = MessageBuffer(10)
    for item in (position(1), static(1), position(2)):
        buffer.put(item)

    assert drain(buffer) == ["p1", "s1", "p2"]


def test_when_full_the_oldest_position_is_dropped_and_static_data_is_kept():
    buffer = MessageBuffer(3)
    for item in (static(1), position(1), position(2), position(3)):
        buffer.put(item)

    assert drain(buffer) == ["s1", "p2", "p3"]
    assert (buffer.dropped_positions, buffer.dropped_static) == (1, 0)


def test_a_buffer_of_static_data_only_drops_its_oldest_message():
    buffer = MessageBuffer(2)
    for item in (static(1), static(2), static(3)):
        buffer.put(item)

    assert drain(buffer) == ["s2", "s3"]
    assert (buffer.dropped_positions, buffer.dropped_static) == (0, 1)


def test_get_waits_for_a_message():
    async def scenario():
        buffer = MessageBuffer(10)
        waiting = asyncio.create_task(buffer.get())
        await asyncio.sleep(0)
        assert not waiting.done()
        buffer.put(position(1))
        return (await asyncio.wait_for(waiting, timeout=1)).raw

    assert asyncio.run(scenario()) == "p1"


def test_close_wakes_a_waiting_get_with_none():
    async def scenario():
        buffer = MessageBuffer(10)
        waiting = asyncio.create_task(buffer.get())
        await asyncio.sleep(0)
        buffer.close()
        return await asyncio.wait_for(waiting, timeout=1)

    assert asyncio.run(scenario()) is None


def test_the_first_drop_of_each_full_episode_is_logged_once(caplog):
    buffer = MessageBuffer(1)
    with caplog.at_level(logging.WARNING):
        for n in range(5):
            buffer.put(position(n))

    assert caplog.text.count("Buffer full") == 1
    assert buffer.dropped_positions == 4
