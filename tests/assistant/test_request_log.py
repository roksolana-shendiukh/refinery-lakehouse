import datetime
import json
import re
from pathlib import Path
from types import SimpleNamespace

from assistant.agent import AgentAnswer, ToolCall
from assistant.prompts import Source
from assistant.request_log import COLUMN_TYPES, RequestLog, insert_statement, log_record
from assistant.retrieval import Chunk

RAG_TABLES_TF = Path(__file__).parents[2] / "infra" / "rag_tables.tf"
ASKED_AT = datetime.datetime(2026, 11, 27, 10, 30, tzinfo=datetime.UTC)
URL = "https://www.eia.gov/outlooks/steo/report/global_oil.php"
ANSWER = AgentAnswer(
    question="What is the Brent forecast?",
    answer=f"About $100/b [STEO]({URL}).",
    model="databricks-meta-llama-3-3-70b-instruct",
    sources=(
        Source("STEO", "2026-11-10", URL, "outlook", cited=True),
        Source("Old article", "2024-01-02", "https://www.eia.gov/a", "article", cited=False),
    ),
    chunks=(
        Chunk("outlook_global_oil#0001", "outlook_global_oil", "outlook", "STEO", "2026-11-10", URL, "...", 0.9),
        Chunk(
            "article_1#0000", "article_1", "article", "Old article", "2024-01-02", "https://www.eia.gov/a", "...", 0.7
        ),
    ),
    tool_calls=(
        ToolCall("search_articles", '{"query": "Brent forecast"}', ("outlook_global_oil#0001", "article_1#0000")),
    ),
    iterations=2,
    prompt_tokens=4731,
    completion_tokens=169,
    latency_ms=6058,
)


def terraform_columns(table):
    resource = RAG_TABLES_TF.read_text(encoding="utf-8").split(f'resource "databricks_sql_table" "{table}"')[1]
    resource = resource.split('resource "databricks_sql_table"')[0]
    return re.findall(r'column \{\s*name\s*=\s*"(\w+)"', resource)


class FakeStatements:
    def __init__(self, state="SUCCEEDED", error=None):
        self.state = state
        self.error = error
        self.calls = []

    def execute_statement(self, **request):
        self.calls.append(request)
        if self.error:
            raise self.error
        status = SimpleNamespace(state=self.state, error=SimpleNamespace(message="TABLE_OR_VIEW_NOT_FOUND"))
        return SimpleNamespace(status=status)


def request_log(statements):
    client = SimpleNamespace(statement_execution=statements)
    return RequestLog(client, "warehouse-1", "c.rag.assistant_requests", now=lambda: ASKED_AT)


def test_record_has_exactly_the_columns_of_the_terraform_table():
    record = log_record(ANSWER, ASKED_AT)

    assert list(record) == list(COLUMN_TYPES)
    assert list(COLUMN_TYPES) == terraform_columns("assistant_requests")


def test_record_keeps_the_searches_the_chunks_and_only_the_cited_pages():
    record = log_record(ANSWER, ASKED_AT, user_email="analyst@example.com", request_id="r-1")

    assert record["request_id"] == "r-1"
    assert record["user_email"] == "analyst@example.com"
    assert json.loads(record["tool_calls"]) == [
        {
            "name": "search_articles",
            "arguments": '{"query": "Brent forecast"}',
            "chunk_ids": ["outlook_global_oil#0001", "article_1#0000"],
        }
    ]
    assert record["retrieved_chunk_ids"] == ["outlook_global_oil#0001", "article_1#0000"]
    assert record["cited_urls"] == [URL]
    assert (record["prompt_tokens"], record["latency_ms"], record["error"]) == (4731, 6058, None)


def test_insert_names_the_table_by_parameter_and_turns_lists_into_arrays():
    statement = insert_statement()

    assert statement.startswith("INSERT INTO IDENTIFIER(:table_name) (request_id, asked_at, user_email,")
    assert "from_json(:cited_urls, 'ARRAY<STRING>')" in statement
    assert "c.rag" not in statement


def test_write_sends_typed_parameters_and_returns_the_request_id():
    statements = FakeStatements()

    request_id = request_log(statements).write(ANSWER, user_email="analyst@example.com")

    (request,) = statements.calls
    parameters = {parameter.name: parameter for parameter in request["parameters"]}
    assert request["warehouse_id"] == "warehouse-1"
    assert request["on_wait_timeout"].value == "CONTINUE"
    assert parameters["table_name"].value == "c.rag.assistant_requests"
    assert parameters["request_id"].value == request_id
    assert (parameters["asked_at"].value, parameters["asked_at"].type) == (ASKED_AT.isoformat(), "TIMESTAMP")
    assert (parameters["iterations"].value, parameters["iterations"].type) == ("2", "INT")
    assert parameters["stopped_at_limit"].value == "false"
    assert json.loads(parameters["cited_urls"].value) == [URL]
    assert parameters["error"].value is None


def test_failed_statement_does_not_raise():
    assert request_log(FakeStatements(state="FAILED")).write(ANSWER) is None


def test_unreachable_warehouse_does_not_raise():
    assert request_log(FakeStatements(error=ConnectionError("warehouse down"))).write(ANSWER) is None
