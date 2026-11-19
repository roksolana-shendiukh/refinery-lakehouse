"""Runs the AIS producer: aisstream -> buffer -> record -> Zerobus. Wiring, heartbeat and clean shutdown only.

Two tasks share the event loop: the reader puts aisstream messages into a bounded buffer and never waits
on Zerobus; the sender takes them out and sends them. Every Zerobus call runs on one dedicated thread,
so a slow call or a backoff never stops the reader from answering websocket pings.
"""

import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from zerobus.sdk.shared import NonRetriableException, ZerobusException

from ais_producer.ack_callback import ProducerAckCallback
from ais_producer.ais_message_pb2 import AisMessage
from ais_producer.ais_source import AisSourceError, read_messages
from ais_producer.backoff import Backoff
from ais_producer.buffer import MessageBuffer
from ais_producer.config import ConfigError, ProducerConfig
from ais_producer.error_policy import Action, decide
from ais_producer.record_mapper import to_record
from ais_producer.zerobus_sink import ZerobusSink

log = logging.getLogger("ais_producer")

EXIT_OK = 0
EXIT_STOPPED_ON_ERROR = 1  # an outage outlasted the retries: starting again later can work
EXIT_BAD_CONFIG = 2  # settings, credentials, rights, table or schema: a person has to fix it, a restart will not


# Runs a blocking Zerobus call off the event loop and waits for it.
SinkCall = Callable[..., Awaitable]


class SinkThread:
    """One dedicated thread for every Zerobus call: calls never overlap, and the event loop never blocks.

    A cancelled call still finishes on the thread, so a later close() cannot run in the middle of it.
    """

    def __init__(self) -> None:
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="zerobus")

    async def __call__(self, function, *args):
        return await asyncio.get_running_loop().run_in_executor(self._executor, function, *args)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)


@dataclass
class Progress:
    sent: int = 0


async def heartbeat(
    callback: ProducerAckCallback,
    sink: ZerobusSink,
    backoff: Backoff,
    buffer: MessageBuffer,
    progress: Progress,
    every_seconds: int,
) -> None:
    """Logs progress every minute; once records are acknowledged again after a recreate, resets the backoff."""
    acknowledged_at_recreate: int | None = None
    while True:
        await asyncio.sleep(every_seconds)
        stats = callback.snapshot()
        log.info(
            "sent %d, buffered %d, dropped %d positions and %d static, durable %d, failed %d, "
            "last offset %s, last ack %.0f s ago",
            progress.sent,
            len(buffer),
            buffer.dropped_positions,
            buffer.dropped_static,
            stats.acknowledged,
            stats.failed,
            stats.last_offset,
            stats.seconds_since_last_ack,
        )
        if sink.recreated:
            if acknowledged_at_recreate is None:
                acknowledged_at_recreate = stats.acknowledged
            elif stats.acknowledged > acknowledged_at_recreate:
                sink.recovered()
                backoff.reset()
                acknowledged_at_recreate = None
                log.info("Zerobus stream healthy again after recreate")


async def send_with_policy(sink: ZerobusSink, record: AisMessage, backoff: Backoff, call: SinkCall) -> int | None:
    """Sends one record. None means sent; a number is the exit code to stop with.

    A retriable Zerobus error waits (exponential backoff with full jitter), recreates the stream and sends
    again, until the backoff budget is spent. Recreating resends the records the old stream did not confirm.
    Only Zerobus errors are handled here (NonRetriableException is a subclass of ZerobusException).
    Any other exception is a bug: it is not caught, so the producer stops with a traceback.
    """
    stream_failed = False
    while True:
        try:
            if stream_failed:
                await call(sink.recreate)
                stream_failed = False
            await call(sink.send, record)
            return None
        except ZerobusException as error:
            if decide(error) is Action.STOP:
                log.error("Stopping: Zerobus refused the records: %s", error)
                return EXIT_BAD_CONFIG
            delay = backoff.next_delay()
            if delay is None:
                log.error("Stopping: Zerobus still failing after %.0f s of retries: %s", backoff.budget_seconds, error)
                return EXIT_STOPPED_ON_ERROR
            log.warning("Zerobus stream failed (%s); recreating it in %.1f s", error, delay)
            await asyncio.sleep(delay)  # a stop request cancels this wait
            stream_failed = True


async def read_into(config: ProducerConfig, buffer: MessageBuffer) -> int:
    """Reads aisstream into the buffer until the subscription is refused. Never waits on Zerobus."""
    try:
        async for envelope in read_messages(
            config.ais_url,
            config.ais_api_key,
            config.bounding_boxes,
            config.message_types,
            config.ping_interval_seconds,
            config.ping_timeout_seconds,
            config.silence_seconds,
        ):
            buffer.put(envelope)
    except AisSourceError as error:
        log.error("Stopping: aisstream refused the subscription: %s", error)
        return EXIT_BAD_CONFIG
    return EXIT_OK


async def send_from(
    buffer: MessageBuffer, sink: ZerobusSink, backoff: Backoff, call: SinkCall, progress: Progress
) -> int:
    """Sends buffered messages to Zerobus until the buffer is closed and empty, or Zerobus stops it."""
    while (envelope := await buffer.get()) is not None:
        exit_code = await send_with_policy(sink, to_record(envelope), backoff, call)
        if exit_code is not None:
            return exit_code
        progress.sent += 1
    return EXIT_OK


async def drain(sender: asyncio.Task, buffer: MessageBuffer, timeout_seconds: int) -> int:
    """Lets the sender send what is left in the buffer, for at most `timeout_seconds`."""
    buffer.close()
    try:
        return await asyncio.wait_for(sender, timeout_seconds)
    except TimeoutError:
        log.error("Stopping: %d buffered messages were not sent within %d s", len(buffer), timeout_seconds)
        return EXIT_OK


async def run(config: ProducerConfig, stop: asyncio.Event) -> int:
    """Reads and sends until an error or until `stop` is set; then drains the buffer and closes the stream."""
    callback = ProducerAckCallback()
    sink = ZerobusSink(config, callback)
    progress = Progress()
    backoff = Backoff(config.retry_base_seconds, config.retry_max_delay_seconds, config.retry_budget_seconds)
    buffer = MessageBuffer(config.buffer_max_messages)
    try:
        sink.open()
    except ZerobusException as error:
        # The SDK already retried stream creation; at startup there is no stream to recreate.
        log.error("Stopping: could not open the Zerobus stream: %s", error)
        return EXIT_BAD_CONFIG if isinstance(error, NonRetriableException) else EXIT_STOPPED_ON_ERROR
    call = SinkThread()
    beat = asyncio.create_task(heartbeat(callback, sink, backoff, buffer, progress, config.heartbeat_seconds))
    reader = asyncio.create_task(read_into(config, buffer))
    sender = asyncio.create_task(send_from(buffer, sink, backoff, call, progress))
    stop_requested = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({reader, sender, stop_requested}, return_when=asyncio.FIRST_COMPLETED)
        if sender.done():
            return sender.result()  # Zerobus stopped it; a bug re-raises here with its traceback
        if stop_requested.done():
            log.info("Stop requested; closing the AIS connection and sending what is buffered")
        reader.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reader
        # A stop gives EXIT_OK; a refused subscription its own exit code; a bug re-raises here.
        exit_code = EXIT_OK if reader.cancelled() else reader.result()
        # A record still waiting for a Zerobus retry when the drain times out is not sent.
        drained = await drain(sender, buffer, config.drain_timeout_seconds)
        return exit_code if exit_code != EXIT_OK else drained
    finally:
        for task in (beat, reader, sender, stop_requested):
            task.cancel()
        try:
            await call(sink.close)  # after any running call on the Zerobus thread; flushes queued records
        except ZerobusException as error:
            log.error("Closing the Zerobus stream failed: %s", error)
        call.shutdown()
        log.info("Sent %d records in total", progress.sent)


def install_stop_handlers(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> None:
    """SIGTERM (kill, docker stop, systemd) and SIGINT (Ctrl+C) set `stop` instead of killing the process."""
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signal_number, stop.set)
        except NotImplementedError:
            # Windows: its event loop has no add_signal_handler. A plain handler runs between bytecodes,
            # so it hands the stop to the loop thread-safely; that also wakes a loop waiting for a frame.
            signal.signal(signal_number, lambda *_: loop.call_soon_threadsafe(stop.set))


async def serve(config: ProducerConfig) -> int:
    """Runs the producer until an error or a stop signal."""
    stop = asyncio.Event()
    install_stop_handlers(asyncio.get_running_loop(), stop)
    return await run(config, stop)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        config = ProducerConfig.from_env()
    except ConfigError as error:
        log.error("%s", error)
        return EXIT_BAD_CONFIG
    log.info("Starting AIS producer for %s", config.table_name)
    return asyncio.run(serve(config))


if __name__ == "__main__":
    sys.exit(main())
