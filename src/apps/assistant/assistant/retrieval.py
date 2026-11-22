"""Search over the EIA article chunks: one query to the vector search index, results as Chunk objects."""

import datetime
import json
from dataclasses import dataclass

# Columns of the index returned with every hit (all synced from rag.eia_article_chunks)
SEARCH_COLUMNS = ("chunk_id", "doc_id", "doc_type", "title", "published_date", "source_url", "chunk_text")
DOC_TYPES = ("article", "outlook")
# HYBRID adds keyword matching to the embedding similarity: names and figures such as "Brent" or "OPEC+"
# count even when the embedding of a long chunk is about something broader
QUERY_TYPE = "HYBRID"
EPOCH = datetime.date(1970, 1, 1)


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    doc_id: str
    doc_type: str
    title: str
    published_date: str  # YYYY-MM-DD
    source_url: str
    text: str
    score: float


def search_filters(doc_type=None, since=None):
    """Vector search filters: only this kind of page, only pages published on or after a date."""
    filters = {}
    if doc_type:
        filters["doc_type"] = doc_type
    if since:
        filters["published_date >="] = since.isoformat()
    return filters


def query_arguments(query, num_results, doc_type=None, since=None, query_type=QUERY_TYPE):
    """Keyword arguments of WorkspaceClient.vector_search_indexes.query_index (besides the index name)."""
    arguments = {
        "columns": list(SEARCH_COLUMNS),
        "query_text": query,
        "query_type": query_type,
        "num_results": num_results,
    }
    filters = search_filters(doc_type, since)
    if filters:
        arguments["filters_json"] = json.dumps(filters)
    return arguments


def date_text(value):
    """A DATE column as YYYY-MM-DD; vector search may return it as text or as days since 1970-01-01."""
    if isinstance(value, int | float):
        return (EPOCH + datetime.timedelta(days=int(value))).isoformat()
    return str(value)[:10]


def parse_response(response):
    """Chunks from a QueryVectorIndexResponse: rows of data_array in the order of manifest.columns, the score last."""
    names = [column.name for column in response.manifest.columns] if response.manifest else []
    rows = response.result.data_array if response.result and response.result.data_array else []
    chunks = []
    for row in rows:
        values = dict(zip(names, row, strict=True))
        chunks.append(
            Chunk(
                chunk_id=values["chunk_id"],
                doc_id=values["doc_id"],
                doc_type=values["doc_type"],
                title=values["title"],
                published_date=date_text(values["published_date"]),
                source_url=values["source_url"],
                text=values["chunk_text"],
                score=float(values.get("score", 0.0)),
            )
        )
    return chunks


class ArticleSearch:
    """search(query, doc_type=None, since=None) -> the most relevant chunks of EIA articles and STEO pages."""

    def __init__(self, client, index_name, num_results, query_type=QUERY_TYPE):
        self.client = client
        self.index_name = index_name
        self.num_results = num_results
        self.query_type = query_type

    def __call__(self, query, doc_type=None, since=None):
        response = self.client.vector_search_indexes.query_index(
            self.index_name, **query_arguments(query, self.num_results, doc_type, since, self.query_type)
        )
        return parse_response(response)
