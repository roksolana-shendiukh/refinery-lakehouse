from ais_producer.backoff import Backoff


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def make(clock: FakeClock, rand: float = 1.0) -> Backoff:
    return Backoff(base_seconds=2, max_delay_seconds=60, budget_seconds=600, clock=clock, rand=lambda: rand)


def test_the_ceiling_doubles_up_to_the_max_delay():
    backoff = make(FakeClock())

    assert [backoff.next_delay() for _ in range(7)] == [2, 4, 8, 16, 32, 60, 60]


def test_full_jitter_picks_a_random_point_below_the_ceiling():
    backoff = make(FakeClock(), rand=0.25)

    assert [backoff.next_delay() for _ in range(3)] == [0.5, 1.0, 2.0]


def test_none_once_the_budget_is_spent():
    clock = FakeClock()
    backoff = make(clock)

    assert backoff.next_delay() == 2
    clock.now = 599
    assert backoff.next_delay() is not None
    clock.now = 600
    assert backoff.next_delay() is None


def test_reset_starts_a_fresh_series_and_budget():
    clock = FakeClock()
    backoff = make(clock)
    backoff.next_delay()
    clock.now = 600
    assert backoff.next_delay() is None

    backoff.reset()

    assert backoff.next_delay() == 2


def test_a_long_outage_does_not_overflow():
    backoff = make(FakeClock())

    delays = [backoff.next_delay() for _ in range(2000)]

    assert max(delays) == 60
