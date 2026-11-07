"""Tracks Zerobus acknowledgements for monitoring. Observes only: recovery is driven by exceptions in the sink.

The SDK calls on_ack / on_error once per queued record, on its own background threads,
so the methods are short, never block, and protect shared counters with a lock.
"""

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from zerobus.sdk.shared import AckCallback

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AckStats:
    acknowledged: int
    failed: int
    last_offset: int | None
    seconds_since_last_ack: float
    last_error: str | None


class ProducerAckCallback(AckCallback):
    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._clock = clock
        self._acknowledged = 0
        self._failed = 0
        self._last_offset: int | None = None
        self._last_ack_at = clock()
        self._last_error: str | None = None

    def on_ack(self, offset: int) -> None:
        with self._lock:
            self._acknowledged += 1
            self._last_offset = offset
            self._last_ack_at = self._clock()

    def on_error(self, offset: int, error_message: str) -> None:
        log.error("Zerobus failed record at offset %s: %s", offset, error_message)
        with self._lock:
            self._failed += 1
            self._last_error = error_message

    def snapshot(self) -> AckStats:
        """A consistent copy of the counters, for the heartbeat log line."""
        with self._lock:
            return AckStats(
                acknowledged=self._acknowledged,
                failed=self._failed,
                last_offset=self._last_offset,
                seconds_since_last_ack=self._clock() - self._last_ack_at,
                last_error=self._last_error,
            )
