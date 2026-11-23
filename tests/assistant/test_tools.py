import datetime
import json

from assistant.retrieval import Chunk
from assistant.tools import SEARCH_TOOL, run_tool

CHUNK = Chunk(
    chunk_id="article_61142#0000",
    doc_id="article_61142",
    doc_type="article",
    title="Brent crude oil prices averaged $19 per barrel less in 2023 than 2022",
    published_date="2024-01-02",
    source_url="https://www.eia.gov/todayinenergy/detail.php?id=61142",
    text="Brent crude oil prices averaged ... (January 2, 2024)\n\nThe price of Brent crude oil averaged $83/b...",
    score=0.72,
)


class FakeSearch:
    def __init__(self, chunks=(CHUNK,)):
        self.chunks = list(chunks)
        self.calls = []

    def __call__(self, query, doc_type=None, since=None):
        self.calls.append({"query": query, "doc_type": doc_type, "since": since})
        return self.chunks


def test_tool_schema_lets_the_model_choose_query_type_and_date_only():
    parameters = SEARCH_TOOL["function"]["parameters"]

    assert set(parameters["properties"]) == {"query", "doc_type", "since"}
    assert parameters["required"] == ["query"]


def test_search_call_runs_the_search_with_the_model_arguments():
    search = FakeSearch()

    text, chunks = run_tool(
        "search_articles", json.dumps({"query": "Brent", "doc_type": "article", "since": "2024-01-01"}), search
    )

    assert search.calls == [{"query": "Brent", "doc_type": "article", "since": datetime.date(2024, 1, 1)}]
    assert chunks == [CHUNK]
    (result,) = json.loads(text)["results"]
    assert result["url"] == CHUNK.source_url
    assert result["type"] == "Today in Energy"


def test_no_results_tell_the_model_not_to_answer_from_memory():
    text, chunks = run_tool("search_articles", json.dumps({"query": "jet fuel in Antarctica"}), FakeSearch([]))

    assert chunks == []
    assert "Do not answer from memory" in json.loads(text)["note"]


def test_unknown_tool_is_an_error_for_the_model():
    text, chunks = run_tool("get_weather", "{}", FakeSearch())

    assert "Unknown tool" in json.loads(text)["error"]
    assert chunks == []


def test_bad_arguments_are_an_error_for_the_model_and_nothing_is_searched():
    search = FakeSearch()

    for arguments in ("not json", "{}", '{"query": "Brent", "doc_type": "blog"}', '{"query": "x", "since": "2024"}'):
        text, _ = run_tool("search_articles", arguments, search)
        assert "error" in json.loads(text)

    assert search.calls == []
