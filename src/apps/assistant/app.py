"""Refinery assistant: a chat page that answers petroleum market questions from EIA articles (Dash).

Wiring only: settings -> agent and request log -> layout -> callbacks.
On Databricks Apps it runs with gunicorn (app.yaml) and `server` is the WSGI entry point; the app's service
principal signs in by itself.
"""

import logging

from assistant.agent import build_agent
from assistant.request_log import RequestLog
from assistant.settings import AssistantSettings, SettingsError
from dash import Dash, html
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


app = Dash(__name__, title="Refinery · Petroleum market assistant")
server = app.server


try:
    settings = AssistantSettings.from_env()
except SettingsError as error:
    log.error("The assistant is not configured: %s", error)
    app.layout = html.Main([html.H1("Refinery assistant"), html.P(f"The assistant is not configured: {error}")])
else:
    workspace_client = WorkspaceClient()
    agent = build_agent(settings, workspace_client)
    request_log = RequestLog(workspace_client, settings.warehouse_id, settings.log_table)
    app.layout = html.Main([html.H1("Refinery assistant"), html.P("Ask a question about the petroleum market.")])


if __name__ == "__main__":
    app.run(debug=True)
