"""Owns the Zerobus stream: open, send, recreate, close. Decides nothing; error_policy does."""

import logging
from collections.abc import Callable

from zerobus.sdk.shared import StreamConfigurationOptions, TableProperties, ZerobusException
from zerobus.sdk.sync import ZerobusSdk

from ais_producer.ack_callback import ProducerAckCallback
from ais_producer.ais_message_pb2 import AisMessage
from ais_producer.config import ProducerConfig

log = logging.getLogger(__name__)


class ZerobusSink:
    def __init__(
        self,
        config: ProducerConfig,
        callback: ProducerAckCallback,
        sdk_factory: Callable[[str, str], ZerobusSdk] = ZerobusSdk,
    ) -> None:
        self._config = config
        self._sdk = sdk_factory(config.zerobus_endpoint, config.workspace_url)
        # The descriptor selects Protobuf; Zerobus checks it against the table when the stream opens.
        self._table = TableProperties(config.table_name, AisMessage.DESCRIPTOR)
        # SDK defaults: automatic recovery on, 4 retries 2 s apart. The flush timeout bounds a clean stop.
        self._options = StreamConfigurationOptions(
            ack_callback=callback, flush_timeout_ms=config.flush_timeout_seconds * 1000
        )
        self._stream = None
        self.recreated = False

    def open(self) -> None:
        self._stream = self._sdk.create_stream(
            self._config.client_id, self._config.client_secret, self._table, self._options
        )
        log.info("Zerobus stream opened for %s", self._config.table_name)

    def send(self, record: AisMessage) -> int:
        """Queues one record and returns its offset at once; durability is reported to the callback."""
        return self._stream.ingest_record_offset(record.SerializeToString())

    def recreate(self) -> None:
        """A fresh stream from the failed one; records that were not acknowledged are sent again.

        If it fails, the failed stream is kept, so the next attempt recreates from it again.
        """
        self._stream = self._sdk.recreate_stream(self._stream)
        self.recreated = True
        log.warning("Zerobus stream recreated")

    def recovered(self) -> None:
        """Called once records are acknowledged again after a recreate, so a later failure may recreate once more."""
        self.recreated = False

    def close(self) -> None:
        """Flushes and closes: on a graceful stop every queued record becomes durable.

        If the flush fails or times out, logs how many records were not acknowledged and re-raises.
        """
        if self._stream is None:
            return
        stream, self._stream = self._stream, None
        try:
            stream.close()
        except ZerobusException:
            log.error("Zerobus did not confirm %d records before the stream closed", _unacknowledged(stream))
            raise
        log.info("Zerobus stream closed")


def _unacknowledged(stream) -> int:
    """Records sent but not confirmed; the SDK can list them only after close. -1 if it cannot tell."""
    try:
        return sum(1 for _ in stream.get_unacked_records())
    except ZerobusException:
        return -1
