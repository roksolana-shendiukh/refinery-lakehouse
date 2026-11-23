"""How one agent.AgentAnswer is shown: the answer, its sources, the retrieved context and the run details."""

import datetime

from dash import dcc, html

KINDS = {"outlook": "STEO", "article": "Today in Energy"}
SWITCHED_OFF = (
    "The assistant is switched off: its article search runs only around demos. "
    "Ask the team to start it with the assistant-start workflow."
)


def readable_date(value):
    """2026-11-10 -> Nov 10, 2026 (the text as is if it is not a date)."""
    try:
        day = datetime.date.fromisoformat(value)
    except (TypeError, ValueError):
        return value or ""
    return f"{day:%b} {day.day}, {day.year}"


def answer_view(answer):
    """The answer as markdown; its [Title](url) links open the EIA page in a new tab."""
    return dcc.Markdown(answer.answer, link_target="_blank")


def source_row(source):
    return html.Div(
        className="source",
        children=[
            html.Span(KINDS.get(source.doc_type, source.doc_type), className="source-kind"),
            html.A(source.title, href=source.url, target="_blank", rel="noopener noreferrer"),
            html.Span(readable_date(source.published_date), className="source-date"),
        ],
    )


def sources_view(sources):
    """Pages the answer cites, then the other pages the search found (folded)."""
    if not sources:
        return []
    cited = [source for source in sources if source.cited]
    other = [source for source in sources if not source.cited]
    children = []
    if cited:
        children += [html.H2("Sources"), *[source_row(source) for source in cited]]
    if other:
        title = "Also found by the search" if cited else "Found by the search (not cited in the answer)"
        children.append(html.Details([html.Summary(f"{title} ({len(other)})"), *[source_row(s) for s in other]]))
    return children


def context_view(chunks):
    """The passages the model read, folded: they show what the answer is based on."""
    if not chunks:
        return []
    passages = [
        html.Div(
            className="chunk",
            children=[
                html.Div(
                    f"{chunk.title} · {readable_date(chunk.published_date)} · score {chunk.score:.2f}",
                    className="chunk-head",
                ),
                chunk.text,
            ],
        )
        for chunk in chunks
    ]
    label = f"{len(chunks)} passage{'s' if len(chunks) != 1 else ''}"
    return html.Details([html.Summary(f"Retrieved context ({label})"), *passages])


def details_view(answer, request_id=None):
    """One line on how the answer was made: model calls, searches, tokens, time, request id."""
    searches = len(answer.tool_calls)
    parts = [
        f"{answer.iterations} model call{'s' if answer.iterations != 1 else ''}",
        f"{searches} search{'es' if searches != 1 else ''}",
        f"{answer.prompt_tokens + answer.completion_tokens:,} tokens",
        f"{answer.latency_ms / 1000:.1f} s",
    ]
    if request_id:
        parts.append(f"request {request_id}")
    return " · ".join(parts)


def switched_off_view():
    return html.Div(SWITCHED_OFF, className="status-off")
