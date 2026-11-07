"""Reads AIS messages from aisstream.io. Knows nothing about Zerobus or Protobuf."""

import json
import logging
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone

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
) -> AsyncIterator[AisEnvelope]:
    """Yields AIS messages forever. websockets reconnects by itself, with exponential backoff."""
    request = subscription(api_key, bounding_boxes, message_types)
    async for websocket in websockets.connect(
        url, ping_interval=ping_interval_seconds, ping_timeout=ping_timeout_seconds
    ):
        try:
            await websocket.send(request)
            log.info("Subscribed to aisstream")
            async for frame in websocket:
                envelope = parse(frame, message_types, datetime.now(timezone.utc))
                if envelope is not None:
                    yield envelope
        except websockets.ConnectionClosed as error:
            log.warning("AIS connection closed (%s); reconnecting", error)
            continue
