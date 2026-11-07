from ais_producer.ack_callback import ProducerAckCallback


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def test_counts_each_acknowledged_record():
    callback = ProducerAckCallback(clock=FakeClock())

    for offset in range(3):
        callback.on_ack(offset)

    stats = callback.snapshot()
    assert stats.acknowledged == 3
    assert stats.last_offset == 2
    assert stats.failed == 0


def test_remembers_failures():
    callback = ProducerAckCallback(clock=FakeClock())

    callback.on_error(7, "stream closed")

    stats = callback.snapshot()
    assert stats.failed == 1
    assert stats.last_error == "stream closed"


def test_measures_time_since_last_ack():
    clock = FakeClock()
    callback = ProducerAckCallback(clock=clock)
    callback.on_ack(0)

    clock.now += 42

    assert callback.snapshot().seconds_since_last_ack == 42
