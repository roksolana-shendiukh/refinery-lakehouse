"""The request log: one row per question in rag.assistant_requests, written through a SQL warehouse.

Logging must never cost the user an answer: a failed write is logged as a warning and the answer is still shown.
The write does not wait for a cold warehouse either: after a short wait the statement goes on running on its own.
"""

import datetime
import json
import logging
import uuid

from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementParameterListItem

logger = logging.getLogger(__name__)

# Columns of rag.assistant_requests (infra/rag_tables.tf) and the SQL type of each parameter
COLUMN_TYPES = {
    "request_id": "STRING",
    "asked_at": "TIMESTAMP",
    "user_email": "STRING",
    "question": "STRING",
    "answer": "STRING",
    "model": "STRING",
    "tool_calls": "STRING",
    "retrieved_chunk_ids": "STRING",  # JSON, turned into ARRAY<STRING> by from_json in the statement
    "cited_urls": "STRING",
    "iterations": "INT",
    "prompt_tokens": "INT",
    "completion_tokens": "INT",
    "latency_ms": "INT",
    "stopped_at_limit": "BOOLEAN",
    "error": "STRING",
}
ARRAY_COLUMNS = {"retrieved_chunk_ids", "cited_urls"}
WAIT_TIMEOUT = "10s"  # the longest the user waits for the log; the statement itself is not cancelled


def log_record(answer, asked_at, user_email=None, request_id=None):
    """The row for one agent.AgentAnswer, as Python values."""
    return {
        "request_id": request_id or str(uuid.uuid4()),
        "asked_at": asked_at,
        "user_email": user_email,
        "question": answer.question,
        "answer": answer.answer,
        "model": answer.model,
        "tool_calls": json.dumps(
            [
                {"name": call.name, "arguments": call.arguments, "chunk_ids": list(call.chunk_ids)}
                for call in answer.tool_calls
            ]
        ),
        "retrieved_chunk_ids": [chunk.chunk_id for chunk in answer.chunks],
        "cited_urls": [source.url for source in answer.sources if source.cited],
        "iterations": answer.iterations,
        "prompt_tokens": answer.prompt_tokens,
        "completion_tokens": answer.completion_tokens,
        "latency_ms": answer.latency_ms,
        "stopped_at_limit": answer.stopped_at_limit,
        "error": answer.error,
    }


def parameter_value(value):
    """A record value as the text of a statement parameter; None stays None (SQL NULL)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return json.dumps(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    return str(value)


def insert_statement():
    """INSERT with named parameters; the table name is a parameter too (IDENTIFIER), never pasted into the SQL."""
    values = [f"from_json(:{name}, 'ARRAY<STRING>')" if name in ARRAY_COLUMNS else f":{name}" for name in COLUMN_TYPES]
    return f"INSERT INTO IDENTIFIER(:table_name) ({', '.join(COLUMN_TYPES)}) VALUES ({', '.join(values)})"


def statement_parameters(table_name, record):
    parameters = [StatementParameterListItem(name="table_name", value=table_name, type="STRING")]
    for name, sql_type in COLUMN_TYPES.items():
        parameters.append(StatementParameterListItem(name=name, value=parameter_value(record[name]), type=sql_type))
    return parameters


class RequestLog:
    def __init__(self, workspace_client, warehouse_id, table_name, now=None):
        self.workspace_client = workspace_client
        self.warehouse_id = warehouse_id
        self.table_name = table_name
        self.now = now or (lambda: datetime.datetime.now(datetime.UTC))

    def write(self, answer, user_email=None):
        """Write one answer to the log. Returns the request id, or None if the write failed."""
        record = log_record(answer, self.now(), user_email)
        try:
            response = self.workspace_client.statement_execution.execute_statement(
                statement=insert_statement(),
                warehouse_id=self.warehouse_id,
                parameters=statement_parameters(self.table_name, record),
                wait_timeout=WAIT_TIMEOUT,
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
            )
        except Exception:  # a broken log must not break the answer
            logger.warning("Could not write request %s to %s", record["request_id"], self.table_name, exc_info=True)
            return None

        state = response.status.state if response.status else None
        state = getattr(state, "value", state)
        if state in ("FAILED", "CANCELED", "CLOSED"):
            error = response.status.error.message if response.status.error else state
            logger.warning("Request %s was not logged: %s", record["request_id"], error)
            return None
        return record["request_id"]
