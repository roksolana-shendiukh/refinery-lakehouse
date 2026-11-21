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
    ensure_endpoint,
    ensure_index,
    wait_for_endpoint,
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

    def get_endpoint(self, name):
        if not self.states:
            raise NotFound(f"Endpoint {name} not found")
        state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return EndpointInfo(name=name, endpoint_status=EndpointStatus(state=state, message=f"{state}"))

    def create_endpoint(self, name, endpoint_type):
        self.created.append((name, endpoint_type))
        self.states = [EndpointStatusState.PROVISIONING, EndpointStatusState.ONLINE]


class FakeIndexes:
    def __init__(self, index=None):
        self.index = index
        self.created = []

    def get_index(self, name):
        if self.index is None:
            raise NotFound(f"Index {name} not found")
        return self.index

    def create_index(self, **args):
        self.created.append(args)


class FakeClient:
    def __init__(self, endpoint_states=(), index=None):
        self.vector_search_endpoints = FakeEndpoints(endpoint_states)
        self.vector_search_indexes = FakeIndexes(index)


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
