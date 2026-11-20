"""Reads AIS messages from aisstream.io. Knows nothing about Zerobus or Protobuf."""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

import websockets

log = logging.getLogger(__name__)


class AisSourceError(RuntimeError):
    """aisstream rejected the subscription (e.g. an invalid API key): reconnecting will not help."""


@dataclass(frozen=True)
class AisEnvelope:
    """One AIS message as received: when it arrived, its type, and the unchanged JSON text."""

    received_at: datetime
    message_type: str
    raw: str


@dataclass
class SourceStats:
    """What the reader has seen, for the heartbeat. Updated and read on the same event loop: no lock needed."""

    connections: int = 0  # subscriptions sent; every one after the first is a reconnect
    disconnects: int = 0  # connections closed by the network or by aisstream
    silences: int = 0  # connections closed by the silent-feed watchdog
    received: int = 0  # data messages read
    last_message_at: float | None = None  # time.monotonic() of the last data message

    def seconds_since_last_message(self) -> float | None:
        return None if self.last_message_at is None else time.monotonic() - self.last_message_at


def subscription(api_key: str, bounding_boxes: Iterable, message_types: Iterable[str]) -> str:
    """The first message aisstream expects after connecting."""
    return json.dumps(
        {
            "APIKey": api_key,
            "BoundingBoxes": [[list(corner) for corner in box] for box in bounding_boxes],
            "FilterMessageTypes": list(message_types),
        }
    )


def parse(frame: str | bytes, message_types: Iterable[str], received_at: datetime) -> AisEnvelope | None:
    """Turns one websocket frame into an envelope; None for frames that are not data (e.g. confirmations)."""
    text = frame.decode("utf-8") if isinstance(frame, bytes) else frame  # aisstream sends binary frames
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        log.warning("Skipping a frame that is not JSON")
        return None
    if "error" in payload:
        raise AisSourceError(payload["error"])
    message_type = payload.get("MessageType")
    if message_type not in message_types:
        return None
    return AisEnvelope(received_at=received_at, message_type=message_type, raw=text.strip())


async def read_messages(
    url: str,
    api_key: str,
    bounding_boxes: Iterable,
    message_types: Iterable[str],
    ping_interval_seconds: int,
    ping_timeout_seconds: int,
    silence_seconds: int,
    stats: SourceStats | None = None,
) -> AsyncIterator[AisEnvelope]:
    """Yields AIS messages forever. websockets reconnects by itself, with exponential backoff.

    Pings only prove the connection is alive, not that aisstream still sends data. If no frame arrives
    for `silence_seconds` while pings still succeed, the connection is closed and opened again.
    """
    stats = stats if stats is not None else SourceStats()
    request = subscription(api_key, bounding_boxes, message_types)
    async for websocket in websockets.connect(
        url, ping_interval=ping_interval_seconds, ping_timeout=ping_timeout_seconds
    ):
        try:
            await websocket.send(request)
            stats.connections += 1
            log.info("Subscribed to aisstream (connection %d)", stats.connections)
            while True:
                try:
                    # Only the wait for a frame is timed, not the time the consumer spends on a message.
                    async with asyncio.timeout(silence_seconds):
                        frame = await websocket.recv()
                except TimeoutError:
                    log.warning("No AIS data for %d s on an open connection; reconnecting", silence_seconds)
                    stats.silences += 1
                    await websocket.close()
                    break
                envelope = parse(frame, message_types, datetime.now(UTC))
                if envelope is not None:
                    stats.received += 1
                    stats.last_message_at = time.monotonic()
                    yield envelope
        except websockets.ConnectionClosed as error:
            stats.disconnects += 1
            log.warning("AIS connection closed (%s); reconnecting", error)
            continue
