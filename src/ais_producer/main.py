"""Runs the AIS producer: aisstream -> record -> Zerobus. Wiring, heartbeat and clean shutdown only"""

import asyncio
import contextlib
import logging
import signal
import sys
from dataclasses import dataclass

from zerobus.sdk.shared import NonRetriableException, ZerobusException

from ais_producer.ack_callback import ProducerAckCallback
from ais_producer.ais_message_pb2 import AisMessage
from ais_producer.ais_source import AisSourceError, read_messages
from ais_producer.backoff import Backoff
from ais_producer.config import ConfigError, ProducerConfig
from ais_producer.error_policy import Action, decide
from ais_producer.record_mapper import to_record
from ais_producer.zerobus_sink import ZerobusSink

log = logging.getLogger("ais_producer")

EXIT_OK = 0
EXIT_STOPPED_ON_ERROR = 1  # an outage outlasted the retries: starting again later can work
EXIT_BAD_CONFIG = 2  # settings, credentials, rights, table or schema: a person has to fix it, a restart will not


@dataclass
class Progress:
    sent: int = 0


async def heartbeat(
    callback: ProducerAckCallback, sink: ZerobusSink, backoff: Backoff, progress: Progress, every_seconds: int
) -> None:
    """Logs progress every minute; once records are acknowledged again after a recreate, resets the backoff."""
    acknowledged_at_recreate: int | None = None
    while True:
        await asyncio.sleep(every_seconds)
        stats = callback.snapshot()
        log.info(
            "sent %d, durable %d, failed %d, last offset %s, last ack %.0f s ago",
            progress.sent,
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


async def send_with_policy(sink: ZerobusSink, record: AisMessage, backoff: Backoff) -> int | None:
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
                sink.recreate()
                stream_failed = False
            sink.send(record)
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


async def forward(config: ProducerConfig, sink: ZerobusSink, backoff: Backoff, progress: Progress) -> int:
    """Reads aisstream and sends every message to Zerobus until an error stops it."""
    try:
        async for envelope in read_messages(
            config.ais_url,
            config.ais_api_key,
            config.bounding_boxes,
            config.message_types,
            config.ping_interval_seconds,
            config.ping_timeout_seconds,
        ):
            exit_code = await send_with_policy(sink, to_record(envelope), backoff)
            if exit_code is not None:
                return exit_code
            progress.sent += 1
    except AisSourceError as error:
        log.error("Stopping: aisstream refused the subscription: %s", error)
        return EXIT_BAD_CONFIG
    return EXIT_OK


async def run(config: ProducerConfig, stop: asyncio.Event) -> int:
    """Forwards messages until an error or until `stop` is set; then closes the stream, which flushes."""
    callback = ProducerAckCallback()
    sink = ZerobusSink(config, callback)
    progress = Progress()
    backoff = Backoff(config.retry_base_seconds, config.retry_max_delay_seconds, config.retry_budget_seconds)
    try:
        sink.open()
    except ZerobusException as error:
        # The SDK already retried stream creation; at startup there is no stream to recreate.
        log.error("Stopping: could not open the Zerobus stream: %s", error)
        return EXIT_BAD_CONFIG if isinstance(error, NonRetriableException) else EXIT_STOPPED_ON_ERROR
    beat = asyncio.create_task(heartbeat(callback, sink, backoff, progress, config.heartbeat_seconds))
    work = asyncio.create_task(forward(config, sink, backoff, progress))
    stop_requested = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({work, stop_requested}, return_when=asyncio.FIRST_COMPLETED)
        if work.done():
            return work.result()  # an error stopped it; a bug re-raises here with its traceback
        # Cancelling interrupts the wait for the next frame or a backoff wait. A record already queued in
        # the SDK is flushed on close; a record still waiting for a retry is not sent.
        log.info("Stop requested; closing the AIS connection and flushing Zerobus")
        work.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await work
        return EXIT_OK
    finally:
        for task in (beat, work, stop_requested):
            task.cancel()
        try:
            sink.close()  # flushes: queued records become durable before exit
        except ZerobusException as error:
            log.error("Closing the Zerobus stream failed: %s", error)
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
