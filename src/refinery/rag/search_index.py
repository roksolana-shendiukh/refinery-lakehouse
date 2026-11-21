"""Vector search for the RAG assistant: one endpoint and one Delta Sync index over the article chunks.

They exist only while they are needed (in PROD, around the demo): the start job creates and syncs them,
the stop job deletes them, because an endpoint is billed for every hour it is up, queried or not.
The jobs use the SDK.
"""

from dataclasses import dataclass

from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    EndpointType,
    PipelineType,
    VectorIndexType,
)

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
