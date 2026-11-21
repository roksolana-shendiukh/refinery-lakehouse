from databricks.sdk.service.vectorsearch import EndpointType, PipelineType, VectorIndexType
from rag_testing import terraform_columns

from refinery.rag.search_index import COLUMNS_TO_SYNC, ENDPOINT_TYPE, SearchConfig, create_index_args

CONFIG = SearchConfig(
    endpoint_name="refinery-rag",
    index_name="refinery_dev.dev_x_rag.eia_article_chunks_index",
    source_table="refinery_dev.dev_x_rag.eia_article_chunks",
    embedding_model="databricks-gte-large-en",
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
