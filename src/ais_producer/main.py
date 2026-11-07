"""Runs the AIS producer: aisstream -> record -> Zerobus. Wiring, heartbeat and clean shutdown only"""

import asyncio
import logging
import sys
from dataclasses import dataclass

from zerobus.sdk.shared import ZerobusException

from ais_producer.ack_callback import ProducerAckCallback
from ais_producer.ais_message_pb2 import AisMessage
from ais_producer.ais_source import AisSourceError, read_messages
from ais_producer.config import ConfigError, ProducerConfig
from ais_producer.error_policy import Action, decide
from ais_producer.record_mapper import to_record
from ais_producer.zerobus_sink import ZerobusSink

log = logging.getLogger("ais_producer")

EXIT_OK = 0
EXIT_STOPPED_ON_ERROR = 1
EXIT_BAD_CONFIG = 2


@dataclass
class Progress:
    sent: int = 0


async def heartbeat(callback: ProducerAckCallback, sink: ZerobusSink, progress: Progress, every_seconds: int) -> None:
    """Logs progress every minute and clears the recreate flag once records are acknowledged again."""
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
                acknowledged_at_recreate = None
                log.info("Zerobus stream healthy again after recreate")


def send_with_policy(sink: ZerobusSink, record: AisMessage) -> bool:
    """Sends one record; on an unrecoverable Zerobus error follows error_policy. False means stop.

    Only Zerobus errors are handled here (NonRetriableException is a subclass of ZerobusException).
    Any other exception is a bug: it is not caught, so the producer stops with a traceback.
    """
    while True:
        try:
            sink.send(record)
            return True
        except ZerobusException as error:
            action = decide(error, sink.recreated)
            if action is Action.RECREATE_STREAM:
                log.warning("Zerobus stream failed (%s); recreating it once", error)
                sink.recreate()
                continue
            log.error("Stopping: %s", error)
            return False


async def run(config: ProducerConfig) -> int:
    callback = ProducerAckCallback()
    sink = ZerobusSink(config, callback)
    progress = Progress()
    sink.open()
    beat = asyncio.create_task(heartbeat(callback, sink, progress, config.heartbeat_seconds))
    try:
        async for envelope in read_messages(
            config.ais_url,
            config.ais_api_key,
            config.bounding_boxes,
            config.message_types,
            config.ping_interval_seconds,
            config.ping_timeout_seconds,
        ):
            if not send_with_policy(sink, to_record(envelope)):
                return EXIT_STOPPED_ON_ERROR
            progress.sent += 1
    except AisSourceError as error:
        log.error("Stopping: aisstream refused the subscription: %s", error)
        return EXIT_STOPPED_ON_ERROR
    finally:
        beat.cancel()
        try:
            sink.close()  # flushes: queued records become durable before exit
        except ZerobusException as error:
            log.error("Closing the Zerobus stream failed: %s", error)
    return EXIT_OK


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        config = ProducerConfig.from_env()
    except ConfigError as error:
        log.error("%s", error)
        return EXIT_BAD_CONFIG
    log.info("Starting AIS producer for %s", config.table_name)
    try:
        return asyncio.run(run(config))
    except KeyboardInterrupt:
        log.info("Stopped by Ctrl+C")
        return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
