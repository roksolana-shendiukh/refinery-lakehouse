import pandas as pd

from data import TANKER_COLUMNS, CachedQuery, QueryError, select_query


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class FakeWarehouse:
    def __init__(self) -> None:
        self.calls = 0
        self.fail = False
        self.queries: list[str] = []

    def __call__(self, query: str) -> pd.DataFrame:
        self.calls += 1
        self.queries.append(query)
        if self.fail:
            raise QueryError("warehouse stopped")
        return pd.DataFrame([{"vessel_name": "SANMAR RAGA", "calls": self.calls}])


def make_source() -> tuple[CachedQuery, FakeWarehouse, FakeClock]:
    warehouse, clock = FakeWarehouse(), FakeClock()
    source = CachedQuery(warehouse, "cat.live.live_tankers", TANKER_COLUMNS, ttl_seconds=15, clock=clock)
    return source, warehouse, clock


def test_query_reads_only_the_listed_columns():
    assert select_query("cat.live.t", ("a", "b")) == "SELECT a, b FROM cat.live.t"


def test_tanker_query_includes_status_and_zone():
    source, warehouse, _ = make_source()

    source.snapshot()

    assert "status" in warehouse.queries[0]
    assert "zone_name" in warehouse.queries[0]


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
    assert snapshot.rows.loc[0, "calls"] == 2


def test_failed_refresh_keeps_the_last_good_data():
    source, warehouse, clock = make_source()
    source.snapshot()
    warehouse.fail = True

    clock.now += 15
    snapshot = source.snapshot()

    assert snapshot.error == "warehouse stopped"
    assert snapshot.rows.loc[0, "vessel_name"] == "SANMAR RAGA"
    assert snapshot.fetched_at == 0.0


def test_failure_before_any_data_gives_an_empty_frame_with_the_columns():
    source, warehouse, _ = make_source()
    warehouse.fail = True

    snapshot = source.snapshot()

    assert snapshot.rows.empty
    assert list(snapshot.rows.columns) == list(TANKER_COLUMNS)
    assert snapshot.fetched_at is None
    assert snapshot.error == "warehouse stopped"
