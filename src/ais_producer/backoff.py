"""How long to wait between Zerobus retries: exponential backoff with full jitter, within a time budget.

Full jitter: attempt n waits a random time between 0 and min(max_delay, base * 2**n), so producers that
fail together do not retry together. The budget bounds the whole retry series; then the producer stops.
"""

import random
import time
from collections.abc import Callable

# 2**30 already exceeds any sensible delay; capping the exponent avoids huge numbers on long outages.
_MAX_EXPONENT = 30


class Backoff:
    def __init__(
        self,
        base_seconds: float,
        max_delay_seconds: float,
        budget_seconds: float,
        clock: Callable[[], float] = time.monotonic,
        rand: Callable[[], float] = random.random,
    ) -> None:
        self._base = base_seconds
        self._max_delay = max_delay_seconds
        self._budget = budget_seconds
        self._clock = clock
        self._rand = rand
        self._attempt = 0
        self._started: float | None = None

    @property
    def budget_seconds(self) -> float:
        return self._budget

    def next_delay(self) -> float | None:
        """Seconds to wait before the next attempt, or None once the budget is spent."""
        now = self._clock()
        if self._started is None:
            self._started = now
        if now - self._started >= self._budget:
            return None
        ceiling = min(self._max_delay, self._base * 2 ** min(self._attempt, _MAX_EXPONENT))
        self._attempt += 1
        return self._rand() * ceiling

    def reset(self) -> None:
        """Called once Zerobus acknowledges records again: the next failure starts a fresh series."""
        self._attempt = 0
        self._started = None
