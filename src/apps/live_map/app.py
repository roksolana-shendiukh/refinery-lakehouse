"""Live map of tankers on the U.S. coasts. UI only: data, settings and presentation rules live in other modules."""

import logging

import pandas as pd
import pydeck as pdk
import streamlit as st
from settings import AppSettings, SettingsError
from styling import dot_color, freshness_line, tooltip_text

from data import TankerSource, warehouse_query

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

st.set_page_config(page_title="Refinery: live tankers", layout="wide")

TABLE_COLUMNS = ["vessel_name", "destination", "sog_knots", "minutes_since_update", "latitude", "longitude"]
US_VIEW = pdk.ViewState(latitude=33.0, longitude=-95.0, zoom=3)


@st.cache_resource
def load() -> tuple[AppSettings, TankerSource]:
    """Created once per app process and shared by every viewer, so the query cache is shared too."""
    settings = AppSettings.from_env()
    source = TankerSource(warehouse_query(settings.warehouse_id), settings.table_name, settings.refresh_seconds)
    return settings, source


def minutes(value) -> int | None:
    return None if pd.isna(value) else int(value)


def show(settings: AppSettings, source: TankerSource) -> None:
    snapshot = source.snapshot()
    tankers = snapshot.tankers.copy()
    newest = None if tankers.empty else minutes(tankers["minutes_since_update"].min())

    st.caption(freshness_line(newest, len(tankers)))
    if snapshot.error:
        st.warning(f"The latest refresh failed, showing the last good data: {snapshot.error}")
    if tankers.empty:
        return

    tankers["color"] = [dot_color(minutes(m), settings.fresh_minutes) for m in tankers["minutes_since_update"]]
    tankers["tooltip"] = [
        tooltip_text(row.vessel_name, row.destination, row.sog_knots, minutes(row.minutes_since_update))
        for row in tankers.itertuples()
    ]
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=tankers,
        get_position=["longitude", "latitude"],
        get_fill_color="color",
        get_radius=8000,
        radius_min_pixels=4,
        pickable=True,
    )
    st.pydeck_chart(
        pdk.Deck(layers=[layer], initial_view_state=US_VIEW, tooltip={"text": "{tooltip}"}, map_style=None),
        use_container_width=True,
    )
    st.dataframe(tankers[TABLE_COLUMNS], hide_index=True, use_container_width=True)


st.title("Tankers on the U.S. coasts: live")

try:
    app_settings, tanker_source = load()
except SettingsError as error:
    st.error(f"The app is not configured: {error}")
    st.stop()


@st.fragment(run_every=app_settings.refresh_seconds)
def live_view() -> None:
    show(app_settings, tanker_source)


live_view()
