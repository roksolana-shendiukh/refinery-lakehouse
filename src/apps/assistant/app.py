"""Refinery assistant: a chat page that answers petroleum market questions from EIA articles (Dash).

Wiring only: settings -> agent and request log -> layout -> callbacks.
On Databricks Apps it runs with gunicorn (app.yaml) and `server` is the WSGI entry point; the app's service
principal signs in by itself.
"""

import logging

from assistant.agent import build_agent
from assistant.layout import not_configured, page
from assistant.request_log import RequestLog
from assistant.settings import AssistantSettings, SettingsError
from dash import Dash
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


FONTS = "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Space+Grotesk:wght@500;600&display=swap"

app = Dash(__name__, title="Refinery · Petroleum market assistant", external_stylesheets=[FONTS])
server = app.server


try:
    settings = AssistantSettings.from_env()
except SettingsError as error:
    log.error("The assistant is not configured: %s", error)
    app.layout = not_configured(str(error))
else:
    workspace_client = WorkspaceClient()
    agent = build_agent(settings, workspace_client)
    request_log = RequestLog(workspace_client, settings.warehouse_id, settings.log_table)
    app.layout = page()


if __name__ == "__main__":
    app.run(debug=True)
