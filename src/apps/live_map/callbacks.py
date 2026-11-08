"""Dash callbacks: what changes on the page, and when.

The map itself is never rebuilt. A refresh replaces only the layer contents (markers and zones, matched by
id), so the viewer's zoom, pan and open tooltip stay as they are. The camera moves only when the viewer
picks a coast, resets the view or opens a terminal.
"""

import time
from collections.abc import Callable

from dash import ALL, Dash, Input, Output, ctx, no_update
from layout import (
    BADGE,
    CHIPS,
    COAST,
    HIGHLIGHT,
    MAP,
    MAP_PANE,
    MAP_TAB,
    MOST_CONGESTED,
    RESET,
    TABLE,
    TANKERS,
    TERMINALS_PANE,
    TERMINALS_TAB,
    TICK,
    VIEW,
    ZONES,
    badge,
    badge_class,
    chips,
    most_congested_text,
)
from map_layers import tanker_markers, zone_bounds, zone_shapes
from settings import AppSettings
from styling import freshness
from summary import summarize
from terminals_table import ROW_TYPE, table, table_frame
from views import ALL_COASTS, coast_camera

from data import Sources

PANE = "pane"
HIDDEN_PANE = "pane pane-hidden"


def pane_classes(view: str | None) -> tuple[str, str]:
    """(map pane, terminals pane) classes. Hidden panes keep their size, so Leaflet never needs a resize."""
    if view == TERMINALS_TAB:
        return HIDDEN_PANE, f"{PANE} terminals-pane"
    return PANE, f"{HIDDEN_PANE} terminals-pane"


def fit_viewport(box: list[list[float]]) -> dict:
    return {"bounds": box, "transition": "flyToBounds", "options": {"padding": [80, 80], "maxZoom": 11}}


def clicked_port(triggered_id, triggered_clicks: int | None) -> str | None:
    """The terminal a viewer clicked; None when the callback fired because the table was redrawn (clicks 0)."""
    if not isinstance(triggered_id, dict) or not triggered_clicks:
        return None
    return triggered_id.get("port")


def register(app: Dash, settings: AppSettings, sources: Sources, clock: Callable[[], float] = time.monotonic) -> None:
    @app.callback(
        Output(TANKERS, "children"),
        Output(CHIPS, "children"),
        Output(MOST_CONGESTED, "children"),
        Output(MOST_CONGESTED, "hidden"),
        Output(BADGE, "children"),
        Output(BADGE, "className"),
        Input(TICK, "n_intervals"),
    )
    def refresh_tankers(_tick):
        tankers = sources.tankers.snapshot()
        terminals = sources.terminals.snapshot()
        summary = summarize(tankers.rows, terminals.rows)
        age = None if tankers.fetched_at is None else clock() - tankers.fetched_at
        fresh = freshness(age, tankers.error or terminals.error, len(tankers.rows))
        congested = most_congested_text(summary)
        return (
            tanker_markers(tankers.rows, settings.fresh_minutes),
            chips(summary),
            congested,
            not congested,
            badge(fresh),
            badge_class(fresh),
        )

    @app.callback(
        Output(ZONES, "children"),
        Input(TICK, "n_intervals"),
        Input(MAP, "zoom"),
        Input(HIGHLIGHT, "data"),
    )
    def refresh_zones(_tick, zoom, highlighted):
        return zone_shapes(sources.zones.snapshot().rows, sources.terminals.snapshot().rows, zoom, highlighted)

    @app.callback(Output(TABLE, "children"), Input(TICK, "n_intervals"), Input(COAST, "value"))
    def refresh_table(_tick, coast):
        return table(table_frame(sources.terminals.snapshot().rows, coast))

    @app.callback(Output(MAP_PANE, "className"), Output(TERMINALS_PANE, "className"), Input(VIEW, "value"))
    def switch_view(view):
        return pane_classes(view)

    @app.callback(
        Output(MAP, "viewport"),
        Output(HIGHLIGHT, "data"),
        Input(COAST, "value"),
        prevent_initial_call=True,
    )
    def fly_to_coast(coast):
        return coast_camera(coast).viewport(), None

    @app.callback(
        Output(COAST, "value"),
        Output(MAP, "viewport", allow_duplicate=True),
        Output(HIGHLIGHT, "data", allow_duplicate=True),
        Input(RESET, "n_clicks"),
        prevent_initial_call=True,
    )
    def reset_view(_clicks):
        return ALL_COASTS, coast_camera(ALL_COASTS).viewport(), None

    @app.callback(
        Output(MAP, "viewport", allow_duplicate=True),
        Output(VIEW, "value"),
        Output(HIGHLIGHT, "data", allow_duplicate=True),
        Input({"type": ROW_TYPE, "port": ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def open_terminal(_clicks):
        port = clicked_port(ctx.triggered_id, ctx.triggered[0]["value"] if ctx.triggered else None)
        box = zone_bounds(sources.zones.snapshot().rows, port) if port else None
        if box is None:
            return no_update, no_update, no_update
        return fit_viewport(box), MAP_TAB, port

    @app.callback(
        Output(VIEW, "value", allow_duplicate=True),
        Input(MOST_CONGESTED, "n_clicks"),
        prevent_initial_call=True,
    )
    def open_terminals(clicks):
        return TERMINALS_TAB if clicks else no_update
