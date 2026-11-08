import pandas as pd

from data import COLUMNS, QueryError, TankerSource, query_for


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class FakeWarehouse:
    def __init__(self) -> None:
        self.calls = 0
        self.fail = False

    def __call__(self, query: str) -> pd.DataFrame:
        self.calls += 1
        if self.fail:
            raise QueryError("warehouse stopped")
        return pd.DataFrame([{"vessel_name": "SANMAR RAGA", "calls": self.calls}])


def make_source() -> tuple[TankerSource, FakeWarehouse, FakeClock]:
    warehouse, clock = FakeWarehouse(), FakeClock()
    return TankerSource(warehouse, "cat.live.live_tankers", ttl_seconds=15, clock=clock), warehouse, clock


def test_query_reads_only_the_needed_columns():
    query = query_for("cat.live.live_tankers")

    assert query.startswith("SELECT mmsi, vessel_name")
    assert "FROM cat.live.live_tankers" in query
    assert len(COLUMNS) == 12


def test_viewers_share_one_query_per_interval():
    source, warehouse, clock = make_source()

    source.snapshot()
    clock.now += 10
    source.snapshot()

    assert warehouse.calls == 1


def test_data_is_refreshed_after_the_interval():
    source, warehouse, clock = make_source()
    source.snapshot()

    clock.now += 15
    snapshot = source.snapshot()

    assert warehouse.calls == 2
    assert snapshot.tankers.loc[0, "calls"] == 2


def test_failed_refresh_keeps_the_last_good_data():
    source, warehouse, clock = make_source()
    source.snapshot()
    warehouse.fail = True

    clock.now += 15
    snapshot = source.snapshot()

    assert snapshot.error == "warehouse stopped"
    assert snapshot.tankers.loc[0, "vessel_name"] == "SANMAR RAGA"
    assert snapshot.fetched_at == 0.0


def test_failure_before_any_data_gives_an_empty_table_and_the_error():
    source, warehouse, _ = make_source()
    warehouse.fail = True

    snapshot = source.snapshot()

    assert snapshot.tankers.empty
    assert snapshot.fetched_at is None
    assert snapshot.error == "warehouse stopped"
