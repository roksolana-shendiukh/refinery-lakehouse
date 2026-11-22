"""The tool the model can call, and how a call is run.

The model only chooses what to search for; the number of results comes from the settings, so a model
cannot ask for the whole index. A bad call (unknown tool, broken arguments) goes back to the model
as an error it can correct, not as an exception that ends the answer.
"""

import datetime
import json

from assistant.retrieval import DOC_TYPES

SEARCH_TOOL_NAME = "search_articles"
SEARCH_TOOL = {
    "type": "function",
    "function": {
        "name": SEARCH_TOOL_NAME,
        "description": (
            "Search EIA (U.S. Energy Information Administration) Today in Energy articles since 2024 and the "
            "current Short-Term Energy Outlook (STEO) sections on oil and petroleum products. Returns the most "
            "relevant passages with title, publication date and link."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "What to look for, in English, e.g. 'Brent crude oil price forecast 2026'.",
                },
                "doc_type": {
                    "type": "string",
                    "enum": list(DOC_TYPES),
                    "description": "Only 'outlook' (STEO: forecasts) or only 'article' (Today in Energy analysis).",
                },
                "since": {
                    "type": "string",
                    "description": "Only passages published on or after this date, YYYY-MM-DD.",
                },
            },
            "required": ["query"],
        },
    },
}
TOOLS = [SEARCH_TOOL]


class ToolCallError(ValueError):
    """The model called a tool that does not exist or with arguments that do not fit."""


def search_arguments(arguments_json):
    """query, doc_type and since from the model's JSON arguments, checked."""
    try:
        arguments = json.loads(arguments_json or "{}")
    except json.JSONDecodeError as error:
        raise ToolCallError(f"Arguments are not valid JSON: {error}") from None
    if not isinstance(arguments, dict):
        raise ToolCallError("Arguments must be a JSON object")

    query = str(arguments.get("query") or "").strip()
    if not query:
        raise ToolCallError("'query' is required")
    doc_type = arguments.get("doc_type") or None
    if doc_type is not None and doc_type not in DOC_TYPES:
        raise ToolCallError(f"'doc_type' must be one of {list(DOC_TYPES)}")
    since = arguments.get("since") or None
    if since is not None:
        try:
            since = datetime.date.fromisoformat(str(since))
        except ValueError:
            raise ToolCallError("'since' must be a date in the form YYYY-MM-DD") from None
    return {"query": query, "doc_type": doc_type, "since": since}


def search_result(chunks):
    """What the model reads after a search: the passages with their source, or a note that nothing was found."""
    if not chunks:
        return json.dumps({"results": [], "note": "No passages found. Do not answer from memory."})
    return json.dumps(
        {
            "results": [
                {
                    "chunk_id": chunk.chunk_id,
                    "title": chunk.title,
                    "published_date": chunk.published_date,
                    "type": "STEO" if chunk.doc_type == "outlook" else "Today in Energy",
                    "url": chunk.source_url,
                    "text": chunk.text,
                }
                for chunk in chunks
            ]
        }
    )


def run_tool(name, arguments_json, search):
    """Run one tool call. Returns (text for the model, chunks found); a bad call returns an error text."""
    try:
        if name != SEARCH_TOOL_NAME:
            raise ToolCallError(f"Unknown tool {name!r}; the only tool is {SEARCH_TOOL_NAME!r}")
        chunks = search(**search_arguments(arguments_json))
    except ToolCallError as error:
        return json.dumps({"error": str(error)}), []
    return search_result(chunks), chunks
