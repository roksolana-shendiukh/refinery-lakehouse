import asyncio
import os
import signal
from datetime import UTC, datetime

import pytest
from zerobus.sdk.shared import ZerobusException

from ais_producer import main
from ais_producer.ais_source import AisEnvelope, AisSourceError
from ais_producer.config import ProducerConfig

CONFIG = ProducerConfig(
    zerobus_endpoint="https://endpoint",
    workspace_url="https://workspace",
    table_name="cat.bronze.zerobus_ais_messages",
    client_id="id",
    client_secret="secret",
    ais_api_key="key",
    heartbeat_seconds=3600,
)

ENVELOPE = AisEnvelope(datetime(2026, 11, 8, tzinfo=UTC), "PositionReport", '{"MessageType": "PositionReport"}')


class FakeSink:
    def __init__(self, config, callback) -> None:
        self.sent = []
        self.closed = False
        self.close_error = None
        self.recreated = False

    def open(self) -> None:
        pass

    def send(self, record) -> int:
        self.sent.append(record)
        return len(self.sent) - 1

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


@pytest.fixture
def sink(monkeypatch):
    """The FakeSink that run() creates, so a test can look at it afterwards."""
    created = []

    def factory(config, callback):
        created.append(FakeSink(config, callback))
        return created[-1]

    monkeypatch.setattr(main, "ZerobusSink", factory)
    return created


def source(*, then_error: Exception | None = None, sent_one: asyncio.Event | None = None):
    """A fake aisstream: one message, then an error or silence (waits forever, like a quiet feed)."""

    async def read_messages(*args):
        yield ENVELOPE
        if sent_one is not None:
            sent_one.set()
        if then_error is not None:
            raise then_error
        await asyncio.Event().wait()

    return read_messages


def test_stop_request_closes_the_stream_and_exits_ok(monkeypatch, sink):
    async def scenario():
        stop, sent_one = asyncio.Event(), asyncio.Event()
        monkeypatch.setattr(main, "read_messages", source(sent_one=sent_one))
        running = asyncio.create_task(main.run(CONFIG, stop))
        await sent_one.wait()
        stop.set()
        return await running

    assert asyncio.run(scenario()) == main.EXIT_OK
    assert len(sink[0].sent) == 1
    assert sink[0].closed


def test_refused_subscription_stops_with_an_error_and_still_closes(monkeypatch, sink):
    monkeypatch.setattr(main, "read_messages", source(then_error=AisSourceError("Api Key Is Not Valid")))

    assert asyncio.run(main.run(CONFIG, asyncio.Event())) == main.EXIT_STOPPED_ON_ERROR
    assert sink[0].closed


def test_a_bug_is_raised_with_its_traceback_after_the_stream_is_closed(monkeypatch, sink):
    monkeypatch.setattr(main, "read_messages", source(then_error=KeyError("bug")))

    with pytest.raises(KeyError):
        asyncio.run(main.run(CONFIG, asyncio.Event()))
    assert sink[0].closed


def test_a_failed_flush_is_logged_and_does_not_change_the_exit_code(monkeypatch, sink, caplog):
    async def scenario():
        stop, sent_one = asyncio.Event(), asyncio.Event()
        monkeypatch.setattr(main, "read_messages", source(sent_one=sent_one))
        running = asyncio.create_task(main.run(CONFIG, stop))
        await sent_one.wait()
        sink[0].close_error = ZerobusException("flush timed out")
        stop.set()
        return await running

    assert asyncio.run(scenario()) == main.EXIT_OK
    assert "Closing the Zerobus stream failed" in caplog.text


@pytest.mark.parametrize("signal_number", [signal.SIGTERM, signal.SIGINT])
def test_sigterm_and_sigint_ask_for_a_clean_stop(monkeypatch, sink, signal_number):
    async def scenario():
        sent_one = asyncio.Event()
        monkeypatch.setattr(main, "read_messages", source(sent_one=sent_one))
        running = asyncio.create_task(main.serve(CONFIG))
        await sent_one.wait()
        os.kill(os.getpid(), signal_number)
        return await asyncio.wait_for(running, timeout=5)

    assert asyncio.run(scenario()) == main.EXIT_OK
    assert sink[0].closed


class LoopWithoutSignalHandlers:
    """Behaves like the Windows event loop: add_signal_handler is not implemented."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def add_signal_handler(self, *args):
        raise NotImplementedError

    def call_soon_threadsafe(self, callback, *args):
        return self._loop.call_soon_threadsafe(callback, *args)


def test_without_add_signal_handler_a_plain_handler_sets_stop():
    previous = signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)

    async def scenario():
        stop = asyncio.Event()
        main.install_stop_handlers(LoopWithoutSignalHandlers(asyncio.get_running_loop()), stop)
        signal.raise_signal(signal.SIGINT)
        await asyncio.wait_for(stop.wait(), timeout=5)
        return stop.is_set()

    try:
        assert asyncio.run(scenario())
    finally:
        signal.signal(signal.SIGINT, previous[0])
        signal.signal(signal.SIGTERM, previous[1])
