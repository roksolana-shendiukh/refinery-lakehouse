"""Vector search for the RAG assistant: one endpoint and one Delta Sync index over the article chunks.

They exist only while they are needed (in PROD, around the demo): the start job creates and syncs them,
the stop job deletes them, because an endpoint is billed for every hour it is up, queried or not.
The jobs use the SDK.
"""

import logging
import time
from dataclasses import dataclass

from databricks.sdk.errors import NotFound
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    EndpointStatusState,
    EndpointType,
    PipelineType,
    VectorIndexType,
)

logger = logging.getLogger(__name__)

# One endpoint per workspace for the assistant
ENDPOINT_NAME = "refinery-rag"
# STANDARD: low-latency queries; STORAGE_OPTIMIZED is for far larger indexes and answers slower
ENDPOINT_TYPE = EndpointType.STANDARD
# Pay-per-token Foundation Model API endpoint; embeds the chunks here and the questions in the agent
EMBEDDING_MODEL = "databricks-gte-large-en"

PRIMARY_KEY = "chunk_id"
TEXT_COLUMN = "chunk_text"
COLUMNS_TO_SYNC = (
    "chunk_id",
    "doc_id",
    "doc_type",
    "title",
    "published_date",
    "source_url",
    "chunk_index",
    "chunk_text",
)

# A new endpoint takes several minutes to come online
ENDPOINT_TIMEOUT_SECONDS = 30 * 60
POLL_SECONDS = 30

# States from which an endpoint does not come online by waiting
ENDPOINT_FAILED_STATES = {EndpointStatusState.OFFLINE, EndpointStatusState.RED_STATE, EndpointStatusState.DELETED}

# Index states (status.detailed_state). ready=True alone is not enough: an online index can have a failed update.
INDEX_SYNCED = "ONLINE_NO_PENDING_UPDATE"
INDEX_FAILED_STATES = {"OFFLINE_FAILED", "ONLINE_PIPELINE_FAILED"}
# A sync can be started only from these; in any other state the index is already provisioning or updating
INDEX_SYNCABLE_STATES = {"ONLINE", INDEX_SYNCED, "ONLINE_PIPELINE_FAILED"}
# Embedding the whole knowledge base on the first sync takes the longest
INDEX_TIMEOUT_SECONDS = 45 * 60
# An endpoint can be deleted only once its index is gone
INDEX_DELETE_TIMEOUT_SECONDS = 10 * 60


@dataclass(frozen=True)
class SearchConfig:
    endpoint_name: str
    index_name: str  # catalog.schema.name, next to the chunks table
    source_table: str  # rag eia_article_chunks, with change data feed
    embedding_model: str  # serving endpoint that embeds the chunks and the questions


def index_spec(config):
    """Delta Sync spec: Databricks embeds chunk_text with the model and syncs only when TRIGGERED."""
    return DeltaSyncVectorIndexSpecRequest(
        source_table=config.source_table,
        pipeline_type=PipelineType.TRIGGERED,
        embedding_source_columns=[
            EmbeddingSourceColumn(name=TEXT_COLUMN, embedding_model_endpoint_name=config.embedding_model)
        ],
        columns_to_sync=list(COLUMNS_TO_SYNC),
    )


def create_index_args(config):
    """Arguments of WorkspaceClient.vector_search_indexes.create_index for the chunks index."""
    return {
        "name": config.index_name,
        "endpoint_name": config.endpoint_name,
        "primary_key": PRIMARY_KEY,
        "index_type": VectorIndexType.DELTA_SYNC,
        "delta_sync_index_spec": index_spec(config),
    }


def find_endpoint(client, name):
    try:
        return client.vector_search_endpoints.get_endpoint(name)
    except NotFound:
        return None


def find_index(client, name):
    try:
        return client.vector_search_indexes.get_index(name)
    except NotFound:
        return None


def endpoint_state(endpoint):
    return endpoint.endpoint_status.state if endpoint and endpoint.endpoint_status else None


def wait_for_endpoint(
    client,
    name,
    timeout_seconds=ENDPOINT_TIMEOUT_SECONDS,
    poll_seconds=POLL_SECONDS,
    sleep=time.sleep,
    clock=time.monotonic,
):
    """Wait until the endpoint is ONLINE; fail at once if it went to a failed state, or after the timeout."""
    deadline = clock() + timeout_seconds
    while True:
        endpoint = find_endpoint(client, name)
        state = endpoint_state(endpoint)
        if state == EndpointStatusState.ONLINE:
            return endpoint
        if endpoint is None or state in ENDPOINT_FAILED_STATES:
            message = endpoint.endpoint_status.message if endpoint and endpoint.endpoint_status else "not found"
            raise RuntimeError(f"Vector search endpoint {name} is {state}: {message}")
        if clock() >= deadline:
            raise TimeoutError(f"Vector search endpoint {name} is still {state} after {timeout_seconds} s")
        logger.info("Endpoint %s is %s; waiting", name, state)
        sleep(poll_seconds)


def ensure_endpoint(client, config, **wait_options):
    """Create the endpoint if it does not exist, then wait until it is online. Returns True if it was created."""
    created = find_endpoint(client, config.endpoint_name) is None
    if created:
        logger.info("Creating vector search endpoint %s", config.endpoint_name)
        client.vector_search_endpoints.create_endpoint(name=config.endpoint_name, endpoint_type=ENDPOINT_TYPE)
    wait_for_endpoint(client, config.endpoint_name, **wait_options)
    return created


def ensure_index(client, config):
    """Create the chunks index if it does not exist. Returns True if it was created.
    An existing index is kept as it is.
    """
    index = find_index(client, config.index_name)
    if index is None:
        logger.info("Creating vector search index %s over %s", config.index_name, config.source_table)
        client.vector_search_indexes.create_index(**create_index_args(config))
        return True

    source_table = index.delta_sync_index_spec.source_table if index.delta_sync_index_spec else None
    if source_table != config.source_table:
        raise RuntimeError(f"Index {config.index_name} reads {source_table}, expected {config.source_table}")
    logger.info("Vector search index %s already exists", config.index_name)
    return False


def index_status(client, name):
    """The index status from the REST API, or None if there is no index."""
    try:
        response = client.api_client.do("GET", f"/api/2.0/vector-search/indexes/{name}")
    except NotFound:
        return None
    return response.get("status") or {}


def wait_for_index(
    client,
    name,
    expected_rows=None,
    timeout_seconds=INDEX_TIMEOUT_SECONDS,
    poll_seconds=POLL_SECONDS,
    sleep=time.sleep,
    clock=time.monotonic,
):
    """Wait until the index has no pending update (and holds expected_rows, if given); fail on a failed update."""
    deadline = clock() + timeout_seconds
    while True:
        status = index_status(client, name)
        if status is None:
            raise RuntimeError(f"Vector search index {name} not found")
        state, rows = status.get("detailed_state"), status.get("indexed_row_count")
        if state == INDEX_SYNCED and status.get("ready") and expected_rows in (None, rows):
            logger.info("Index %s is synced: %s rows", name, rows)
            return status
        if state in INDEX_FAILED_STATES:
            raise RuntimeError(f"Vector search index {name} is {state}: {status.get('message')}")
        if clock() >= deadline:
            raise TimeoutError(f"Vector search index {name} is {state} with {rows} of {expected_rows} rows")
        logger.info("Index %s is %s with %s rows; waiting", name, state, rows)
        sleep(poll_seconds)


def sync_index(client, config, expected_rows=None, poll_seconds=POLL_SECONDS, sleep=time.sleep, **wait_options):
    """Start a sync of the index (unless it is already provisioning or updating) and wait until it is done.
    expected_rows is the row count of the chunks table: the sync is done when the index holds exactly that many.
    """
    state = (index_status(client, config.index_name) or {}).get("detailed_state")
    if state in INDEX_SYNCABLE_STATES:
        logger.info("Syncing index %s (was %s)", config.index_name, state)
        client.vector_search_indexes.sync_index(config.index_name)
        # The state turns from "no pending update" to "updating" only after a moment; do not read it too early.
        sleep(poll_seconds)
    else:
        logger.info(
            "Index %s is %s; waiting for the running update instead of starting a sync", config.index_name, state
        )
    return wait_for_index(
        client, config.index_name, expected_rows, poll_seconds=poll_seconds, sleep=sleep, **wait_options
    )


def delete_index(
    client,
    name,
    timeout_seconds=INDEX_DELETE_TIMEOUT_SECONDS,
    poll_seconds=POLL_SECONDS,
    sleep=time.sleep,
    clock=time.monotonic,
):
    """Delete the index and wait until it is gone. Returns False if there was no index."""
    if find_index(client, name) is None:
        logger.info("Vector search index %s does not exist", name)
        return False
    logger.info("Deleting vector search index %s", name)
    try:
        client.vector_search_indexes.delete_index(name)
    except NotFound:
        return False

    deadline = clock() + timeout_seconds
    while find_index(client, name) is not None:
        if clock() > deadline:
            raise TimeoutError(f"Vector search index {name} still exists {timeout_seconds} s after deleting it")
        sleep(poll_seconds)
    return True


def delete_endpoint(client, name):
    """Delete the endpoint if it serves no other index. Returns False if there was no endpoint.
    An endpoint with an index we did not create is somebody else's: it is an error, not something to delete.
    """
    if find_endpoint(client, name) is None:
        logger.info("Vector search endpoint %s does not exist", name)
        return False
    other_indexes = [index.name for index in client.vector_search_indexes.list_indexes(name)]
    if other_indexes:
        raise RuntimeError(f"Vector search endpoint {name} still serves {other_indexes}; it is not deleted")
    logger.info("Deleting vector search endpoint %s", name)
    try:
        client.vector_search_endpoints.delete_endpoint(name)
    except NotFound:
        return False
    return True


def stop_search(client, config, **wait_options):
    """Delete the index, then the endpoint: nothing is billed for vector search afterward."""
    return {
        "index_deleted": delete_index(client, config.index_name, **wait_options),
        "endpoint_deleted": delete_endpoint(client, config.endpoint_name),
    }


def start_search(client, config, expected_rows, **wait_options):
    """Create what is missing (endpoint, index) and sync the index with the chunks table."""
    endpoint_created = ensure_endpoint(client, config, **wait_options)
    index_created = ensure_index(client, config)
    status = sync_index(client, config, expected_rows, **wait_options)
    return {
        "endpoint_created": endpoint_created,
        "index_created": index_created,
        "indexed_rows": status.get("indexed_row_count"),
    }


def refresh_search(client, config, expected_rows, **wait_options):
    """Sync the index after the chunks changed, if the index exists."""
    if find_index(client, config.index_name) is None:
        logger.info("Vector search index %s does not exist; nothing to sync", config.index_name)
        return {"synced": False}
    status = sync_index(client, config, expected_rows, **wait_options)
    return {"synced": True, "indexed_rows": status.get("indexed_row_count")}
