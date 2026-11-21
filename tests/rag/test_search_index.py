from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecResponse,
    EndpointInfo,
    EndpointStatus,
    EndpointStatusState,
    EndpointType,
    PipelineType,
    VectorIndex,
    VectorIndexType,
)
from rag_testing import terraform_columns

from refinery.rag.search_index import (
    COLUMNS_TO_SYNC,
    ENDPOINT_TYPE,
    SearchConfig,
    create_index_args,
    delete_endpoint,
    delete_index,
    ensure_endpoint,
    ensure_index,
    stop_search,
    sync_index,
    wait_for_endpoint,
    wait_for_index,
)

CONFIG = SearchConfig(
    endpoint_name="refinery-rag",
    index_name="refinery_dev.dev_x_rag.eia_article_chunks_index",
    source_table="refinery_dev.dev_x_rag.eia_article_chunks",
    embedding_model="databricks-gte-large-en",
)


class FakeEndpoints:
    """vector_search_endpoints: the endpoint goes through the given states, one per get_endpoint call."""

    def __init__(self, states=()):
        self.states = list(states)
        self.created = []
        self.deleted = []

    def get_endpoint(self, name):
        if not self.states:
            raise NotFound(f"Endpoint {name} not found")
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return EndpointInfo(name=name, endpoint_status=EndpointStatus(state=state, message=f"{state}"))

    def create_endpoint(self, name, endpoint_type):
        self.created.append((name, endpoint_type))
        self.states = [EndpointStatusState.PROVISIONING, EndpointStatusState.ONLINE]

    def delete_endpoint(self, name):
        self.deleted.append(name)
        self.states = []


class FakeIndexes:
    """vector_search_indexes: a deleted index stays visible for delete_polls more get_index calls."""

    def __init__(self, index=None, other_indexes=(), delete_polls=0):
        self.index = index
        self.other_indexes = list(other_indexes)
        self.delete_polls = delete_polls
        self.created = []
        self.synced = []
        self.deleted = []

    def get_index(self, name):
        if self.deleted and self.delete_polls == 0:
            self.index = None
        elif self.deleted:
            self.delete_polls -= 1
        if self.index is None:
            raise NotFound(f"Index {name} not found")
        return self.index

    def delete_index(self, name):
        self.deleted.append(name)

    def list_indexes(self, endpoint_name):
        return [SimpleNamespace(name=name) for name in self.other_indexes]

    def create_index(self, **args):
        self.created.append(args)

    def sync_index(self, name):
        self.synced.append(name)


class FakeApiClient:
    """api_client.do for GET /vector-search/indexes: the index goes through (detailed_state, rows) statuses."""

    def __init__(self, statuses=()):
        self.statuses = list(statuses)

    def do(self, method, path):
        if not self.statuses:
            raise NotFound(f"{path} not found")
        state, rows = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        return {"status": {"detailed_state": state, "ready": state.startswith("ONLINE"), "indexed_row_count": rows}}


class FakeClient:
    def __init__(self, endpoint_states=(), index=None, index_statuses=(), other_indexes=(), delete_polls=0):
        self.vector_search_endpoints = FakeEndpoints(endpoint_states)
        self.vector_search_indexes = FakeIndexes(index, other_indexes, delete_polls)
        self.api_client = FakeApiClient(index_statuses)


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def no_wait():
    clock = FakeClock()
    return {"sleep": clock.sleep, "clock": clock}


def existing_index(source_table=CONFIG.source_table):
    return VectorIndex(
        name=CONFIG.index_name, delta_sync_index_spec=DeltaSyncVectorIndexSpecResponse(source_table=source_table)
    )


def test_endpoint_is_standard_for_low_latency_queries():
    assert ENDPOINT_TYPE == EndpointType.STANDARD


def test_index_is_a_triggered_delta_sync_index_keyed_by_chunk_id():
    args = create_index_args(CONFIG)

    assert args["name"] == CONFIG.index_name
    assert args["endpoint_name"] == "refinery-rag"
    assert args["primary_key"] == "chunk_id"
    assert args["index_type"] == VectorIndexType.DELTA_SYNC
    assert args["delta_sync_index_spec"].source_table == CONFIG.source_table
    assert args["delta_sync_index_spec"].pipeline_type == PipelineType.TRIGGERED


def test_databricks_embeds_the_chunk_text_with_the_configured_model():
    (column,) = create_index_args(CONFIG)["delta_sync_index_spec"].embedding_source_columns

    assert column.name == "chunk_text"
    assert column.embedding_model_endpoint_name == "databricks-gte-large-en"


def test_synced_columns_exist_in_the_chunks_table_and_leave_out_processed_at():
    columns = terraform_columns("eia_article_chunks")

    assert set(COLUMNS_TO_SYNC) <= set(columns)
    assert set(columns) - set(COLUMNS_TO_SYNC) == {"processed_at"}


def test_missing_endpoint_is_created_and_awaited():
    client = FakeClient()

    assert ensure_endpoint(client, CONFIG, **no_wait()) is True
    assert client.vector_search_endpoints.created == [("refinery-rag", EndpointType.STANDARD)]


def test_online_endpoint_is_reused():
    client = FakeClient(endpoint_states=[EndpointStatusState.ONLINE])

    assert ensure_endpoint(client, CONFIG, **no_wait()) is False
    assert client.vector_search_endpoints.created == []


def test_waiting_polls_until_the_endpoint_is_online():
    client = FakeClient(
        endpoint_states=[EndpointStatusState.PROVISIONING, EndpointStatusState.PROVISIONING, EndpointStatusState.ONLINE]
    )
    clock = FakeClock()

    wait_for_endpoint(client, "refinery-rag", poll_seconds=30, sleep=clock.sleep, clock=clock)

    assert clock.now == 60


def test_waiting_stops_at_once_on_a_failed_endpoint():
    client = FakeClient(endpoint_states=[EndpointStatusState.RED_STATE])

    with pytest.raises(RuntimeError, match="RED_STATE"):
        wait_for_endpoint(client, "refinery-rag", **no_wait())


def test_waiting_gives_up_after_the_timeout():
    client = FakeClient(endpoint_states=[EndpointStatusState.PROVISIONING])

    with pytest.raises(TimeoutError, match="PROVISIONING"):
        wait_for_endpoint(client, "refinery-rag", timeout_seconds=120, poll_seconds=30, **no_wait())


def test_missing_index_is_created_from_the_config():
    client = FakeClient()

    assert ensure_index(client, CONFIG) is True
    assert client.vector_search_indexes.created == [create_index_args(CONFIG)]


def test_existing_index_over_the_chunks_table_is_kept():
    client = FakeClient(index=existing_index())

    assert ensure_index(client, CONFIG) is False
    assert client.vector_search_indexes.created == []


def test_existing_index_over_another_table_is_an_error():
    client = FakeClient(index=existing_index("refinery_dev.other.table"))

    with pytest.raises(RuntimeError, match="other.table"):
        ensure_index(client, CONFIG)


def test_sync_starts_an_update_and_waits_until_all_chunks_are_indexed():
    client = FakeClient(
        index_statuses=[
            ("ONLINE_NO_PENDING_UPDATE", 545),
            ("ONLINE_TRIGGERED_UPDATE", 545),
            ("ONLINE_NO_PENDING_UPDATE", 550),
        ]
    )

    status = sync_index(client, CONFIG, expected_rows=550, **no_wait())

    assert client.vector_search_indexes.synced == [CONFIG.index_name]
    assert status["indexed_row_count"] == 550


def test_sync_of_a_new_index_waits_for_its_first_snapshot_without_starting_another():
    client = FakeClient(
        index_statuses=[
            ("PROVISIONING_INITIAL_SNAPSHOT", 0),
            ("PROVISIONING_INITIAL_SNAPSHOT", 0),
            ("ONLINE_NO_PENDING_UPDATE", 545),
        ]
    )

    sync_index(client, CONFIG, expected_rows=545, **no_wait())

    assert client.vector_search_indexes.synced == []


def test_waiting_for_the_index_does_not_stop_before_every_row_is_indexed():
    client = FakeClient(index_statuses=[("ONLINE_NO_PENDING_UPDATE", 500), ("ONLINE_NO_PENDING_UPDATE", 545)])
    clock = FakeClock()

    wait_for_index(client, CONFIG.index_name, expected_rows=545, poll_seconds=30, sleep=clock.sleep, clock=clock)

    assert clock.now == 30


def test_failed_index_update_is_an_error():
    client = FakeClient(index_statuses=[("ONLINE_TRIGGERED_UPDATE", 0), ("ONLINE_PIPELINE_FAILED", 0)])

    with pytest.raises(RuntimeError, match="ONLINE_PIPELINE_FAILED"):
        wait_for_index(client, CONFIG.index_name, expected_rows=545, **no_wait())


def test_waiting_for_the_index_gives_up_after_the_timeout():
    client = FakeClient(index_statuses=[("ONLINE_TRIGGERED_UPDATE", 100)])

    with pytest.raises(TimeoutError, match="100 of 545 rows"):
        wait_for_index(client, CONFIG.index_name, expected_rows=545, timeout_seconds=60, **no_wait())


def test_stop_deletes_the_index_then_the_endpoint():
    client = FakeClient(endpoint_states=[EndpointStatusState.ONLINE], index=existing_index())

    assert stop_search(client, CONFIG, **no_wait()) == {"index_deleted": True, "endpoint_deleted": True}
    assert client.vector_search_indexes.deleted == [CONFIG.index_name]
    assert client.vector_search_endpoints.deleted == ["refinery-rag"]


def test_stop_with_nothing_left_is_not_an_error():
    client = FakeClient()

    assert stop_search(client, CONFIG, **no_wait()) == {"index_deleted": False, "endpoint_deleted": False}


def test_index_deletion_waits_until_the_index_is_gone():
    client = FakeClient(index=existing_index(), delete_polls=2)
    clock = FakeClock()

    assert delete_index(client, CONFIG.index_name, poll_seconds=30, sleep=clock.sleep, clock=clock) is True
    assert clock.now == 60


def test_index_deletion_gives_up_after_the_timeout():
    client = FakeClient(index=existing_index(), delete_polls=100)

    with pytest.raises(TimeoutError, match="still exists"):
        delete_index(client, CONFIG.index_name, timeout_seconds=60, **no_wait())


def test_endpoint_that_serves_another_index_is_not_deleted():
    client = FakeClient(endpoint_states=[EndpointStatusState.ONLINE], other_indexes=["main.other.index"])

    with pytest.raises(RuntimeError, match="main.other.index"):
        delete_endpoint(client, "refinery-rag")
    assert client.vector_search_endpoints.deleted == []
