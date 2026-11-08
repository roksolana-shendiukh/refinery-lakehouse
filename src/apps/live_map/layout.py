"""The page structure: header (title, tabs, coast filter, badge), the map tab with its overlays, and the
Terminals tab. Built once; callbacks only change the parts that carry an id below.
"""

import dash_leaflet as dl
from dash import dcc, html
from settings import AppSettings
from styling import Freshness
from summary import Summary
from views import ALL_COASTS, COASTS, MAX_ZOOM, MIN_ZOOM, coast_camera

MAP_TAB = "Map"
TERMINALS_TAB = "Terminals"

# Component ids used by the callbacks
VIEW = "view"
COAST = "coast"
BADGE = "badge"
MAP = "map"
ZONES = "zones-layer"
TANKERS = "tankers-layer"
CHIPS = "chips"
MOST_CONGESTED = "most-congested"
RESET = "reset-view"
MAP_PANE = "map-pane"
TERMINALS_PANE = "terminals-pane"
TABLE = "terminals-table"
TICK = "tick"
HIGHLIGHT = "highlighted-terminal"

LEGEND = html.Details(
    open=True,
    className="legend",
    children=[
        html.Summary("Legend"),
        html.Div(
            className="legend-items",
            children=[
                html.Span([html.I(className="mk mk-underway"), "Underway"]),
                html.Span([html.I(className="mk mk-waiting"), "Waiting at anchor"]),
                html.Span([html.I(className="mk mk-berth"), "At berth"]),
                html.Span([html.I(className="zn zn-high"), "Terminal: high congestion"]),
                html.Span([html.I(className="zn zn-elevated"), "Elevated"]),
                html.Span([html.I(className="zn zn-normal"), "Normal"]),
                html.Span("Faded = no update for a while", className="legend-note"),
            ],
        ),
    ],
)


def chips(summary: Summary) -> list:
    def chip(value: int, label: str, kind: str = "") -> html.Div:
        return html.Div([html.B(value), html.Span(label)], className=f"chip {kind}".strip())

    return [
        chip(summary.tankers, "tankers"),
        chip(summary.underway, "underway", "chip-underway"),
        chip(summary.waiting, "waiting", "chip-waiting"),
        chip(summary.at_berth, "at berth"),
    ]


def most_congested_text(summary: Summary) -> str:
    if summary.most_congested is None:
        return ""
    return f"Most congested: {summary.most_congested} · {summary.most_congested_waiting} waiting"


def badge(fresh: Freshness) -> list:
    return [html.I(), fresh.text]


def badge_class(fresh: Freshness) -> str:
    return f"badge badge-{fresh.kind}"


def _segmented(component_id: str, options: tuple[str, ...], value: str, label: str) -> html.Div:
    """Radio items styled as a segmented control (see assets/style.css); the wrapper names the group."""
    radios = dcc.RadioItems(
        id=component_id,
        options=list(options),
        value=value,
        inline=True,
        className="segmented",
    )
    return html.Div(radios, role="group", **{"aria-label": label})


def _header() -> html.Header:
    return html.Header(
        className="app-header",
        children=[
            html.H1("Refinery · Live tanker traffic"),
            _segmented(VIEW, (MAP_TAB, TERMINALS_TAB), MAP_TAB, "View"),
            html.Div(className="spacer"),
            _segmented(COAST, COASTS, ALL_COASTS, "Coast"),
            html.Span(id=BADGE, className="badge badge-empty", children=[html.I(), "Loading…"]),
        ],
    )


def _map_pane(settings: AppSettings) -> html.Div:
    start = coast_camera(ALL_COASTS)
    return html.Div(
        id=MAP_PANE,
        className="pane",
        children=[
            dl.Map(
                id=MAP,
                center=[start.latitude, start.longitude],
                zoom=start.zoom,
                zoomSnap=0.25,
                minZoom=MIN_ZOOM,
                maxZoom=MAX_ZOOM,
                zoomControl=False,
                preferCanvas=True,
                trackViewport=True,
                className="map",
                children=[
                    dl.TileLayer(url=settings.tile_url, attribution=settings.tile_attribution),
                    dl.ZoomControl(position="topright"),
                    dl.LayerGroup(id=ZONES),
                    dl.LayerGroup(id=TANKERS),
                ],
            ),
            html.Div(
                className="overlay-top",
                children=[
                    html.Div(id=CHIPS, className="chips"),
                    html.Button(id=MOST_CONGESTED, className="chip chip-alert", n_clicks=0, hidden=True),
                ],
            ),
            html.Button("⟲", id=RESET, className="reset-view", title="Reset to all U.S. coasts", n_clicks=0),
            LEGEND,
        ],
    )


def _terminals_pane() -> html.Div:
    return html.Div(
        id=TERMINALS_PANE,
        className="pane pane-hidden terminals-pane",
        children=[
            html.Div(
                className="terminals-title",
                children=[
                    html.H2("Terminal zones, ranked by tankers waiting"),
                    html.Span("Click a terminal to open it on the map"),
                ],
            ),
            html.Div(id=TABLE, className="table-box"),
            html.P(
                "Waiting = at anchor or almost stopped inside the zone · At berth = moored · "
                "Inbound = destination matches the terminal's port code.",
                className="terminals-note",
            ),
        ],
    )


def page(settings: AppSettings) -> html.Div:
    return html.Div(
        className="app",
        children=[
            _header(),
            html.Main(className="app-main", children=[_map_pane(settings), _terminals_pane()]),
            dcc.Interval(id=TICK, interval=settings.refresh_seconds * 1000),
            dcc.Store(id=HIGHLIGHT),
        ],
    )


def not_configured(message: str) -> html.Div:
    return html.Div(className="app app-error", children=[html.H1("The app is not configured"), html.P(message)])
