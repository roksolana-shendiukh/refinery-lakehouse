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