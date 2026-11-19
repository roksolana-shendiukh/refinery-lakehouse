"""Bounded queue between the aisstream reader and the Zerobus sender. The reader never waits on it.

If Zerobus is slow or down, the reader keeps reading (so websocket pings are answered) and the buffer
fills. When it is full, the oldest PositionReport is dropped: a newer position of the same vessel replaces
it. ShipStaticData is kept, because it is rare and dim_vessel needs it; only a buffer holding nothing but
static data drops its oldest message.
"""

import asyncio
import logging
from collections import deque

from ais_producer.ais_source import AisEnvelope

log = logging.getLogger(__name__)

POSITION_REPORT = "PositionReport"


class MessageBuffer:
    def __init__(self, max_messages: int) -> None:
        self._items: deque[AisEnvelope] = deque()
        self._max = max_messages
        self._not_empty = asyncio.Event()
        self._closed = False
        self._dropping = False
        self.dropped_positions = 0
        self.dropped_static = 0

    def __len__(self) -> int:
        return len(self._items)

    def put(self, envelope: AisEnvelope) -> None:
        """Adds a message at once; when full, first drops one as the module docstring says."""
        if len(self._items) >= self._max:
            self._drop_one()
        self._items.append(envelope)
        self._not_empty.set()

    async def get(self) -> AisEnvelope | None:
        """The oldest message; waits while the buffer is empty. None once it is closed and empty."""
        while not self._items:
            if self._closed:
                return None
            self._dropping = False  # emptied: the next drop is a new episode and is logged again
            self._not_empty.clear()
            await self._not_empty.wait()
        return self._items.popleft()

    def close(self) -> None:
        """No more messages will come: get() returns what is left, then None."""
        self._closed = True
        self._not_empty.set()

    def _drop_one(self) -> None:
        # Scans from the oldest: positions are most of the traffic, so one is found near the front.
        for index, item in enumerate(self._items):
            if item.message_type == POSITION_REPORT:
                del self._items[index]
                self.dropped_positions += 1
                break
        else:
            self._items.popleft()
            self.dropped_static += 1
        if not self._dropping:
            self._dropping = True
            log.warning("Buffer full (%d messages): dropping the oldest positions until Zerobus catches up", self._max)
