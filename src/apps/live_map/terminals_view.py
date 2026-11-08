"""The Terminals tab: terminal zones ranked by tankers waiting. Clicking a row opens it on the map."""

from collections.abc import Callable

import pandas as pd
import streamlit as st
from styling import LEVEL_LABELS
from views import ALL_COASTS

TABLE_KEY = "terminals_table"
ROW_HEIGHT_PX = 35  # Streamlit's default data frame row height

DISPLAY_COLUMNS = {
    "port_name": "Terminal",
    "coast_label": "Coast",
    "waiting": "Waiting",
    "at_berth": "At berth",
    "inbound": "Inbound",
    "waiting_bar": "Congestion",
    "level": "Level",
}


def table_frame(terminals: pd.DataFrame, coast: str | None) -> pd.DataFrame:
    """Rows for the table, filtered by coast and ranked by waiting, then at berth."""
    if terminals.empty:
        return pd.DataFrame(columns=["port_bk", *DISPLAY_COLUMNS])
    rows = terminals.copy()
    if coast and coast != ALL_COASTS:
        rows = rows[rows["coast"] == coast]
    rows = rows.sort_values(["waiting", "at_berth", "port_name"], ascending=[False, False, True])
    rows["coast_label"] = rows["coast"] + " · " + rows["padd"]
    rows["waiting_bar"] = rows["waiting"]
    rows["level"] = rows["congestion_level"].map(LEVEL_LABELS).fillna(rows["congestion_level"])
    return rows[["port_bk", *DISPLAY_COLUMNS]].reset_index(drop=True)


def selected_port(frame: pd.DataFrame) -> str | None:
    """The port code of the row the viewer clicked, from the table's selection state."""
    state = st.session_state.get(TABLE_KEY)
    rows = state.selection.rows if state else []
    if not rows or rows[0] >= len(frame):
        return None
    return str(frame.iloc[rows[0]]["port_bk"])


def render(frame: pd.DataFrame, on_select: Callable[[], None]) -> None:
    st.markdown(
        '<div class="terminals-title"><h2>Terminal zones, ranked by tankers waiting</h2>'
        "<span>Click a terminal to open it on the map</span></div>",
        unsafe_allow_html=True,
    )
    top_waiting = int(frame["waiting"].max()) if not frame.empty else 0
    st.dataframe(
        frame.rename(columns=DISPLAY_COLUMNS),
        key=TABLE_KEY,
        on_select=on_select,
        selection_mode="single-row",
        hide_index=True,
        height=(len(frame) + 1) * ROW_HEIGHT_PX + 3,  # every row, no inner scroll; the tab area scrolls instead
        column_order=list(DISPLAY_COLUMNS.values()),
        column_config={
            "Congestion": st.column_config.ProgressColumn(
                "Congestion", min_value=0, max_value=max(top_waiting, 1), format="%d"
            ),
        },
    )
    st.markdown(
        '<p class="terminals-note">Waiting = at anchor or almost stopped inside the zone · '
        "At berth = moored · Inbound = destination matches the terminal's port code.</p>",
        unsafe_allow_html=True,
    )
