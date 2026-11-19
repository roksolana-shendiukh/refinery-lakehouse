import asyncio
import os
import signal
import sys
import threading
from datetime import UTC, datetime

import pytest
from zerobus.sdk.shared import NonRetriableException, ZerobusException

from ais_producer import main
from ais_producer.ack_callback import ProducerAckCallback
from ais_producer.ais_source import AisEnvelope, AisSourceError, SourceStats
from ais_producer.buffer import MessageBuffer
from ais_producer.config import ProducerConfig

CONFIG = ProducerConfig(
    zerobus_endpoint="https://endpoint",
    workspace_url="https://workspace",
    table_name="cat.bronze.zerobus_ais_messages",
    client_id="id",
    client_secret="secret",
    ais_api_key="key",
    heartbeat_seconds=3600,
    drain_timeout_seconds=1,
)

ENVELOPE = AisEnvelope(datetime(2026, 11, 8, tzinfo=UTC), "PositionReport", '{"MessageType": "PositionReport"}')


class FakeSink:
    def __init__(self, config=None, callback=None) -> None:
        self.sent = []
        self.closed = False
        self.close_error = None
        self.recreated = False
        self.recreates = 0
        self.send_errors: list[Exception] = []  # raised by the next sends, one each
        self.recreate_errors: list[Exception] = []
        self.gate = threading.Event()  # send() waits for it, on the Zerobus thread; open by default
        self.gate.set()
        self.threads: set[str] = set()

    def open(self) -> None:
        pass

    def send(self, record) -> int:
        self.threads.add(threading.current_thread().name)
        self.gate.wait(timeout=5)
        if self.send_errors:
            raise self.send_errors.pop(0)
        self.sent.append(record)
        return len(self.sent) - 1

    def recreate(self) -> None:
        if self.recreate_errors:
            raise self.recreate_errors.pop(0)
        self.recreates += 1
        self.recreated = True

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


def source(*, then_error: Exception | None = None, sent_one: asyncio.Event | None = None, count: int = 1):
    """A fake aisstream: `count` messages, then an error or silence (waits forever, like a quiet feed)."""

    async def read_messages(*args):
        for _ in range(count):
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
    assert len(sink[0].sent) == 1  # read, then sent while draining at the latest
    assert sink[0].closed


def test_refused_subscription_stops_with_bad_config_and_still_closes(monkeypatch, sink):
    monkeypatch.setattr(main, "read_messages", source(then_error=AisSourceError("Api Key Is Not Valid")))

    assert asyncio.run(main.run(CONFIG, asyncio.Event())) == main.EXIT_BAD_CONFIG
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


@pytest.mark.skipif(sys.platform == "win32", reason="os.kill with SIGTERM ends the process on Windows")
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


class FakeBackoff:
    """Delays from a list; None (budget spent) once the list is used up."""

    def __init__(self, delays: list[float]) -> None:
        self.delays = list(delays)
        self.budget_seconds = 600

    def next_delay(self) -> float | None:
        return self.delays.pop(0) if self.delays else None

    def reset(self) -> None:
        pass


RECORD = object()


async def direct(function, *args):
    """Runs a sink call on the event loop itself: enough for send_with_policy tests."""
    return function(*args)


def test_retriable_failures_wait_recreate_and_send_again():
    fake = FakeSink()
    fake.send_errors = [ZerobusException("stream failed"), ZerobusException("stream failed")]

    assert asyncio.run(main.send_with_policy(fake, RECORD, FakeBackoff([0, 0, 0]), direct)) is None
    assert fake.recreates == 2
    assert fake.sent == [RECORD]


def test_a_failed_recreate_is_retried_too():
    fake = FakeSink()
    fake.send_errors = [ZerobusException("stream failed")]
    fake.recreate_errors = [ZerobusException("still down")]

    assert asyncio.run(main.send_with_policy(fake, RECORD, FakeBackoff([0, 0]), direct)) is None
    assert fake.recreates == 1
    assert fake.sent == [RECORD]


def test_spent_budget_stops_with_an_error():
    fake = FakeSink()
    fake.send_errors = [ZerobusException("stream failed")] * 3

    assert asyncio.run(main.send_with_policy(fake, RECORD, FakeBackoff([0, 0]), direct)) == main.EXIT_STOPPED_ON_ERROR
    assert fake.sent == []


def test_non_retriable_error_stops_at_once_with_bad_config():
    fake = FakeSink()
    fake.send_errors = [NonRetriableException("PERMISSION_DENIED")]

    assert asyncio.run(main.send_with_policy(fake, RECORD, FakeBackoff([0]), direct)) == main.EXIT_BAD_CONFIG
    assert fake.recreates == 0


def test_stop_request_interrupts_the_backoff_wait(monkeypatch, sink):
    async def scenario():
        stop, waiting = asyncio.Event(), asyncio.Event()

        class LongBackoff(FakeBackoff):
            def next_delay(self) -> float | None:
                waiting.set()
                return 3600

        monkeypatch.setattr(main, "read_messages", source())
        monkeypatch.setattr(main, "Backoff", lambda *args: LongBackoff([]))
        running = asyncio.create_task(main.run(CONFIG, stop))
        while not sink:
            await asyncio.sleep(0)
        sink[0].send_errors = [ZerobusException("stream failed")]
        await asyncio.wait_for(waiting.wait(), timeout=5)  # the first send now waits 3600 s to recreate
        stop.set()
        return await asyncio.wait_for(running, timeout=5)

    assert asyncio.run(scenario()) == main.EXIT_OK
    assert sink[0].recreates == 0
    assert sink[0].closed


def test_sink_calls_run_on_the_zerobus_thread(monkeypatch, sink):
    async def scenario():
        stop, sent_one = asyncio.Event(), asyncio.Event()
        monkeypatch.setattr(main, "read_messages", source(sent_one=sent_one))
        running = asyncio.create_task(main.run(CONFIG, stop))
        await sent_one.wait()
        stop.set()
        return await running

    asyncio.run(scenario())
    assert sink[0].threads and all(name.startswith("zerobus") for name in sink[0].threads)


def test_the_reader_keeps_reading_while_zerobus_is_retrying(monkeypatch, sink):
    async def scenario():
        stop, all_read, waiting = asyncio.Event(), asyncio.Event(), asyncio.Event()

        class LongBackoff(FakeBackoff):
            def next_delay(self) -> float | None:
                waiting.set()
                return 3600

        monkeypatch.setattr(main, "read_messages", source(sent_one=all_read, count=100))
        monkeypatch.setattr(main, "Backoff", lambda *args: LongBackoff([]))
        running = asyncio.create_task(main.run(CONFIG, stop))
        while not sink:
            await asyncio.sleep(0)
        sink[0].send_errors = [ZerobusException("stream failed")]
        await asyncio.wait_for(waiting.wait(), timeout=5)  # the sender now waits 3600 s to recreate
        await asyncio.wait_for(all_read.wait(), timeout=5)  # ...and the reader still reads all 100
        stop.set()
        return await asyncio.wait_for(running, timeout=5)

    assert asyncio.run(scenario()) == main.EXIT_OK  # the drain times out after 1 s; still a clean stop
    assert sink[0].sent == []
    assert sink[0].closed


def test_a_stop_sends_what_is_buffered(monkeypatch, sink):
    async def scenario():
        stop, all_read = asyncio.Event(), asyncio.Event()
        monkeypatch.setattr(main, "read_messages", source(sent_one=all_read, count=50))
        running = asyncio.create_task(main.run(CONFIG, stop))
        while not sink:
            await asyncio.sleep(0)
        sink[0].gate.clear()  # Zerobus is slow: the first send waits, the rest stays in the buffer
        await asyncio.wait_for(all_read.wait(), timeout=5)
        stop.set()
        await asyncio.sleep(0.05)
        sink[0].gate.set()
        return await asyncio.wait_for(running, timeout=5)

    assert asyncio.run(scenario()) == main.EXIT_OK
    assert len(sink[0].sent) == 50


def test_a_refused_subscription_still_sends_what_is_buffered(monkeypatch, sink):
    monkeypatch.setattr(main, "read_messages", source(count=5, then_error=AisSourceError("Api Key Is Not Valid")))

    assert asyncio.run(main.run(CONFIG, asyncio.Event())) == main.EXIT_BAD_CONFIG
    assert len(sink[0].sent) == 5



def test_heartbeat_logs_one_line_for_aisstream_and_one_for_zerobus(caplog):
    source = SourceStats(connections=3, disconnects=1, silences=1, received=42)
    buffer = MessageBuffer(10)
    buffer.put(ENVELOPE)
    progress = main.Progress(sent=40)

    async def scenario():
        beat = asyncio.create_task(
            main.heartbeat(ProducerAckCallback(), FakeSink(), FakeBackoff([]), buffer, source, progress, 0)
        )
        await asyncio.sleep(0.01)
        beat.cancel()

    with caplog.at_level("INFO"):
        asyncio.run(scenario())

    assert "aisstream: received 42, reconnects 2 (dropped 1, silent 1), last message never" in caplog.text
    assert "zerobus: sent 40, buffered 1, dropped 0 positions and 0 static" in caplog.text
    
