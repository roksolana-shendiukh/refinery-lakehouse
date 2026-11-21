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

# STANDARD: low-latency queries; STORAGE_OPTIMIZED is for far larger indexes and answers slower
ENDPOINT_TYPE = EndpointType.STANDARD
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
