"""What happens on the page: a question (typed or an example) goes to the agent, the answer is shown and logged.
answer_question holds the logic and is tested with fakes; register only wires it to the Dash components.
"""

import logging
import threading
import uuid

from dash import ALL, Input, Output, State, ctx
from databricks.sdk.errors import NotFound
from flask import request

from assistant.layout import ANSWER, ASK, CONTEXT, DETAILS, EXAMPLE, EXAMPLE_QUESTIONS, QUESTION, SOURCES, STATUS
from assistant.views import answer_view, context_view, details_view, sources_view, switched_off_view

logger = logging.getLogger(__name__)

# Databricks Apps puts the signed-in viewer's email in this header; it is absent when the app runs locally
USER_EMAIL_HEADER = "X-Forwarded-Email"
EMPTY_QUESTION = "Type a question or pick one of the examples."


def index_available(workspace_client, index_name):
    """Whether the vector search index exists.
    Only a missing index means "switched off". Any other error (no permission, the API down) is left to the
    agent, so it shows up as an error in the request log.
    """
    try:
        workspace_client.vector_search_indexes.get_index(index_name)
    except NotFound:
        return False
    return True


def run_in_background(function, *args):
    """Run the request log write without making the viewer wait for the SQL warehouse."""
    threading.Thread(target=function, args=args, daemon=True).start()


def answer_question(question, agent, request_log, search_available, user_email=None, background=run_in_background):
    """The page parts for one question: {component id: children}."""
    empty = {STATUS: None, ANSWER: None, SOURCES: [], CONTEXT: [], DETAILS: None}
    question = (question or "").strip()
    if not question:
        return {**empty, STATUS: EMPTY_QUESTION}
    if not search_available():
        return {**empty, STATUS: switched_off_view()}

    answer = agent.ask(question)
    request_id = str(uuid.uuid4())
    background(request_log.write, answer, user_email, request_id)
    return {
        STATUS: None,
        ANSWER: answer_view(answer),
        SOURCES: sources_view(answer.sources),
        CONTEXT: context_view(answer.chunks),
        DETAILS: details_view(answer, request_id),
    }


def chosen_question(triggered_id, typed_question):
    """The typed question after a click on Ask, or the text of the example that was clicked."""
    if isinstance(triggered_id, dict) and triggered_id.get("type") == EXAMPLE:
        return EXAMPLE_QUESTIONS[triggered_id["index"]]
    return typed_question


def register(app, agent, request_log, search_available):
    parts = (STATUS, ANSWER, SOURCES, CONTEXT, DETAILS)

    @app.callback(
        [Output(QUESTION, "value"), *[Output(part, "children") for part in parts]],
        Input(ASK, "n_clicks"),
        Input({"type": EXAMPLE, "index": ALL}, "n_clicks"),
        State(QUESTION, "value"),
        prevent_initial_call=True,
    )
    def ask(_ask_clicks, _example_clicks, typed_question):
        question = chosen_question(ctx.triggered_id, typed_question)
        logger.info("Question: %s", question)
        shown = answer_question(question, agent, request_log, search_available, request.headers.get(USER_EMAIL_HEADER))
        return [question, *[shown[part] for part in parts]]
