"""Live map of tankers on the U.S. coasts. Wiring only: settings -> data -> views.

Two tabs: Map (full window, overlays on top) and Terminals (ranked table). The page never scrolls.
The camera lives in Session State, so the coast filter, the zoom buttons and a click on a terminal
survive the refresh; only mouse-wheel zoom is lost when the map redraws.
"""

import logging
import time
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import pandas as pd
import streamlit as st
import terminals_view
from layout import (
    COAST_KEY,
    LEGEND_HTML,
    MAP_TAB,
    TAB_KEY,
    TERMINALS_TAB,
    badge_html,
    kpi_html,
    most_congested_label,
    render_header,
)
from map_view import build_deck
from settings import AppSettings, SettingsError
from styling import freshness
from summary import summarize
from views import ALL_COASTS, coast_camera, terminal_camera, zoomed
from zones import ZoneShapeError, bounds, parse_polygon

from data import TANKER_COLUMNS, TERMINAL_COLUMNS, ZONE_COLUMNS, CachedQuery, warehouse_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

st.set_page_config(page_title="Refinery · Live tanker traffic", layout="wide", initial_sidebar_state="collapsed")
st.html(f"<style>{(Path(__file__).parent / 'theme.css').read_text(encoding='utf-8')}</style>")

CAMERA_KEY = "camera"
HIGHLIGHT_KEY = "highlighted_terminal"
APP_RERUN_KEY = "needs_app_rerun"


@dataclass(frozen=True)
class Sources:
    tankers: CachedQuery
    terminals: CachedQuery
    zones: CachedQuery


@st.cache_resource
def load() -> tuple[AppSettings, Sources]:
    """Created once per app process and shared by every viewer, so the query caches are shared too."""
    settings = AppSettings.from_env()
    run = warehouse_query(settings.warehouse_id)
    sources = Sources(
        tankers=CachedQuery(run, settings.tankers_table, TANKER_COLUMNS, settings.refresh_seconds),
        terminals=CachedQuery(run, settings.terminals_table, TERMINAL_COLUMNS, settings.refresh_seconds),
        zones=CachedQuery(run, settings.zones_table, ZONE_COLUMNS, settings.zones_refresh_seconds),
    )
    return settings, sources


# Callbacks: they only change Session State; the next run draws the result.


def on_coast_change() -> None:
    st.session_state[CAMERA_KEY] = coast_camera(st.session_state[COAST_KEY])
    st.session_state[HIGHLIGHT_KEY] = None


def on_zoom(steps: float) -> None:
    st.session_state[CAMERA_KEY] = zoomed(st.session_state[CAMERA_KEY], steps)


def on_reset() -> None:
    st.session_state[COAST_KEY] = ALL_COASTS
    on_coast_change()


def switch_tab(tab: str) -> None:
    st.session_state[TAB_KEY] = tab
    st.session_state[APP_RERUN_KEY] = True  # the tabs are outside the fragment


def on_terminal_select(frame: pd.DataFrame, zones: pd.DataFrame) -> None:
    port = terminals_view.selected_port(frame)
    match = zones[zones["port_bk"] == port]
    if port is None or match.empty:
        return
    try:
        st.session_state[CAMERA_KEY] = terminal_camera(bounds(parse_polygon(match.iloc[0]["boundary_wkt"])))
    except ZoneShapeError:
        return
    st.session_state[HIGHLIGHT_KEY] = port
    switch_tab(MAP_TAB)


# Drawing


def draw_map(settings: AppSettings, tankers: pd.DataFrame, terminals: pd.DataFrame, zones: pd.DataFrame) -> None:
    deck = build_deck(
        tankers,
        terminals,
        zones,
        st.session_state[CAMERA_KEY],
        settings.fresh_minutes,
        st.session_state.get(HIGHLIGHT_KEY),
    )
    with st.container(key="map_area"):
        st.pydeck_chart(deck, height="stretch")

    summary = summarize(tankers, terminals)
    with st.container(key="map_kpis", horizontal=True, gap="small"):
        st.html(kpi_html(summary))
        label = most_congested_label(summary)
        if label:
            st.button(label, key="most_congested", on_click=switch_tab, args=(TERMINALS_TAB,))
    st.html(LEGEND_HTML)
    with st.container(key="zoom_controls", gap="small"):
        st.button("+", key="zoom_in", help="Zoom in", on_click=on_zoom, args=(1,))
        st.button("−", key="zoom_out", help="Zoom out", on_click=on_zoom, args=(-1,))
        st.button("⟲", key="zoom_reset", help="Reset to all U.S. coasts", on_click=on_reset)


def draw_terminals(terminals: pd.DataFrame, zones: pd.DataFrame, coast: str) -> None:
    frame = terminals_view.table_frame(terminals, coast)
    with st.container(key="terminals_area"):
        terminals_view.render(frame, on_select=partial(on_terminal_select, frame, zones))


try:
    app_settings, app_sources = load()
except SettingsError as error:
    st.error(f"The app is not configured: {error}")
    st.stop()

st.session_state.setdefault(CAMERA_KEY, coast_camera(None))
render_header(on_coast_change)


@st.fragment(run_every=app_settings.refresh_seconds)
def live_content() -> None:
    if st.session_state.pop(APP_RERUN_KEY, False):
        st.rerun()
    tankers = app_sources.tankers.snapshot()
    terminals = app_sources.terminals.snapshot()
    zones = app_sources.zones.snapshot()

    age = None if tankers.fetched_at is None else time.monotonic() - tankers.fetched_at
    error = tankers.error or terminals.error or zones.error
    st.html(badge_html(freshness(age, error, len(tankers.rows))))

    # Read from Session State, not from the header's return value: a fragment rerun does not rerun the header.
    if st.session_state[TAB_KEY] == MAP_TAB:
        draw_map(app_settings, tankers.rows, terminals.rows, zones.rows)
    else:
        draw_terminals(terminals.rows, zones.rows, st.session_state[COAST_KEY])


live_content()
