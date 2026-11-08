"""Live map of tankers on the U.S. coasts (Dash + Leaflet). Wiring only: settings -> data -> layout -> callbacks.

Run on Databricks Apps with gunicorn (see app.yaml); `server` is the WSGI entry point.
"""

import logging

from callbacks import register
from dash import Dash
from layout import not_configured, page
from settings import AppSettings, SettingsError

from data import TANKER_COLUMNS, TERMINAL_COLUMNS, ZONE_COLUMNS, CachedQuery, Sources, warehouse_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

FONTS = "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Space+Grotesk:wght@500;600&display=swap"

app = Dash(__name__, title="Refinery · Live tanker traffic", external_stylesheets=[FONTS])
server = app.server


def build_sources(settings: AppSettings) -> Sources:
    """One cache per view, shared by every viewer of this process."""
    run = warehouse_query(settings.warehouse_id)
    return Sources(
        tankers=CachedQuery(run, settings.tankers_table, TANKER_COLUMNS, settings.refresh_seconds),
        terminals=CachedQuery(run, settings.terminals_table, TERMINAL_COLUMNS, settings.refresh_seconds),
        zones=CachedQuery(run, settings.zones_table, ZONE_COLUMNS, settings.zones_refresh_seconds),
    )


try:
    app_settings = AppSettings.from_env()
except SettingsError as error:
    log.error("The app is not configured: %s", error)
    app.layout = not_configured(str(error))
else:
    app.layout = page(app_settings)
    register(app, app_settings, build_sources(app_settings))


if __name__ == "__main__":
    app.run(debug=False)
