"""Page chrome: the header (title, tabs, coast filter) and the overlays on the map (key figures, legend, badge).

The HTML here is static markup for styling only; every value in it is a number or escaped text.
"""

import html
from collections.abc import Callable

import streamlit as st
from styling import Freshness
from summary import Summary
from views import ALL_COASTS, COAST_CAMERAS

MAP_TAB = "Map"
TERMINALS_TAB = "Terminals"
TABS = (MAP_TAB, TERMINALS_TAB)

TAB_KEY = "view_tab"
COAST_KEY = "coast"

LEGEND_HTML = """
<details class="legend" open>
  <summary>Legend</summary>
  <div class="legend-items">
    <span><i class="mk mk-underway"></i>Underway</span>
    <span><i class="mk mk-waiting"></i>Waiting at anchor</span>
    <span><i class="mk mk-berth"></i>At berth</span>
    <span><i class="zn zn-high"></i>Terminal: high congestion</span>
    <span><i class="zn zn-elevated"></i>Elevated</span>
    <span><i class="zn zn-normal"></i>Normal</span>
    <span class="legend-note">Faded = no update for a while</span>
  </div>
</details>
"""


def chip(value: int, label: str, kind: str = "") -> str:
    return f'<div class="chip {kind}"><b>{value}</b><span>{html.escape(label)}</span></div>'


def kpi_html(summary: Summary) -> str:
    return "".join(
        [
            chip(summary.tankers, "tankers"),
            chip(summary.underway, "underway", "chip-underway"),
            chip(summary.waiting, "waiting", "chip-waiting"),
            chip(summary.at_berth, "at berth"),
        ]
    )


def most_congested_label(summary: Summary) -> str | None:
    if summary.most_congested is None:
        return None
    return f"Most congested: {summary.most_congested} · {summary.most_congested_waiting} waiting"


def badge_html(freshness: Freshness) -> str:
    return f'<span class="badge badge-{freshness.kind}"><i></i>{html.escape(freshness.text)}</span>'


def render_header(on_coast_change: Callable[[], None]) -> tuple[str, str]:
    """Draws the header and returns the selected tab and coast."""
    # Defaults go through Session State (not default=), because callbacks also set these keys.
    st.session_state.setdefault(TAB_KEY, MAP_TAB)
    st.session_state.setdefault(COAST_KEY, ALL_COASTS)
    with st.container(key="app_header", horizontal=True, vertical_alignment="center"):
        st.markdown('<h1 class="app-title">Refinery · Live tanker traffic</h1>', unsafe_allow_html=True)
        st.segmented_control("View", TABS, required=True, key=TAB_KEY, label_visibility="collapsed")
        st.space("stretch")
        st.segmented_control(
            "Coast",
            tuple(COAST_CAMERAS),
            required=True,
            key=COAST_KEY,
            on_change=on_coast_change,
            label_visibility="collapsed",
        )
    return st.session_state[TAB_KEY], st.session_state[COAST_KEY]
