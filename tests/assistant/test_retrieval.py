import datetime
import json
from types import SimpleNamespace

from assistant.retrieval import SEARCH_COLUMNS, ArticleSearch, Chunk, parse_response, query_arguments


class FakeIndexes:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def query_index(self, index_name, **arguments):
        self.calls.append((index_name, arguments))
        return self.response


class FakeClient:
    def __init__(self, response):
        self.vector_search_indexes = FakeIndexes(response)


def response(*rows):
    """A QueryVectorIndexResponse-like object: manifest.columns[].name and result.data_array."""
    columns = [SimpleNamespace(name=name) for name in [*SEARCH_COLUMNS, "score"]]
    return SimpleNamespace(manifest=SimpleNamespace(columns=columns), result=SimpleNamespace(data_array=list(rows)))


ROW = [
    "outlook_global_oil#0001",
    "outlook_global_oil",
    "outlook",
    "Short-Term Energy Outlook: Global oil markets",
    "2026-11-10",
    "https://www.eia.gov/outlooks/steo/report/global_oil.php",
    "Short-Term Energy Outlook: Global oil markets (November 10, 2026)\n\nWe forecast Brent...",
    0.91,
]


def test_query_asks_for_the_chunk_columns_with_hybrid_search():
    arguments = query_arguments("Brent forecast", 5)

    assert arguments == {
        "columns": list(SEARCH_COLUMNS),
        "query_text": "Brent forecast",
        "query_type": "HYBRID",
        "num_results": 5,
    }


def test_query_filters_by_page_type_and_date():
    arguments = query_arguments("Brent forecast", 5, doc_type="outlook", since=datetime.date(2026, 1, 1))

    assert json.loads(arguments["filters_json"]) == {"doc_type": "outlook", "published_date >=": "2026-01-01"}


def test_response_rows_become_chunks():
    (chunk,) = parse_response(response(ROW))

    assert chunk == Chunk(
        chunk_id="outlook_global_oil#0001",
        doc_id="outlook_global_oil",
        doc_type="outlook",
        title="Short-Term Energy Outlook: Global oil markets",
        published_date="2026-11-10",
        source_url="https://www.eia.gov/outlooks/steo/report/global_oil.php",
        text="Short-Term Energy Outlook: Global oil markets (November 10, 2026)\n\nWe forecast Brent...",
        score=0.91,
    )


def test_date_given_as_days_since_1970_is_read_as_a_date():
    row = list(ROW)
    row[4] = 20402  # 2025-11-10

    assert parse_response(response(row))[0].published_date == "2025-11-10"


def test_empty_response_is_no_chunks():
    empty = SimpleNamespace(manifest=SimpleNamespace(columns=[]), result=SimpleNamespace(data_array=None))

    assert parse_response(empty) == []


def test_search_queries_the_index_with_the_sdk():
    client = FakeClient(response(ROW))
    search = ArticleSearch(client, "c.rag.eia_article_chunks_index", num_results=3)

    chunks = search("Brent forecast", doc_type="outlook")

    ((index_name, arguments),) = client.vector_search_indexes.calls
    assert index_name == "c.rag.eia_article_chunks_index"
    assert arguments["num_results"] == 3
    assert [chunk.chunk_id for chunk in chunks] == ["outlook_global_oil#0001"]
