"""Builds the pydeck map: terminal zones coloured by congestion, tankers by status, labels for busy zones.

Pure: takes data frames and a camera, returns a pydeck Deck. Streamlit only draws the result.
pydeck reads a plain string as a data column name, so constant strings are quoted twice: "'pixels'".
"""

import pandas as pd
import pydeck as pdk
from styling import LEVEL_LABELS, level_style, marker_style, tooltip_html
from views import Camera
from zones import ZoneShapeError, bounds, parse_polygon

TOOLTIP_STYLE = {
    "backgroundColor": "#0B1220",
    "color": "#B7C3D6",
    "border": "1px solid #3A4A66",
    "borderRadius": "10px",
    "padding": "10px 12px",
    "fontFamily": "Inter, system-ui, sans-serif",
    "fontSize": "13px",
    "lineHeight": "1.5",
    "boxShadow": "0 8px 24px rgba(0,0,0,0.45)",
}


def _int_or_none(value) -> int | None:
    return None if pd.isna(value) else int(value)


def _float_or_none(value) -> float | None:
    return None if pd.isna(value) else float(value)


def _str_or_none(value) -> str | None:
    return None if pd.isna(value) or value == "" else str(value)


ELEVATED_LABEL_MIN_ZOOM = 5.0  # below this, only high-congestion zones are labelled, so labels do not overlap


def zone_rows(zones: pd.DataFrame, terminals: pd.DataFrame, highlighted: str | None, zoom: float) -> list[dict]:
    """One row per zone that has a valid polygon, with its congestion colours and label."""
    by_port = terminals.set_index("port_bk").to_dict("index") if not terminals.empty else {}
    rows = []
    for zone in zones.itertuples():
        try:
            ring = parse_polygon(zone.boundary_wkt)
        except ZoneShapeError:
            continue
        terminal = by_port.get(zone.port_bk, {})
        level = terminal.get("congestion_level") or "normal"
        style = level_style(level)
        waiting = int(terminal.get("waiting") or 0)
        rows.append(
            {
                "port_bk": zone.port_bk,
                "polygon": ring,
                "fill": style["fill"],
                "line": style["line"],
                "center": list(bounds(ring).center),
                "label": f"{terminal.get('port_name', zone.port_bk)} · {waiting} waiting",
                "show_label": level == "high"
                or (level == "elevated" and zoom >= ELEVATED_LABEL_MIN_ZOOM)
                or zone.port_bk == highlighted,
                "level": LEVEL_LABELS.get(level, level),
            }
        )
    return rows


def tanker_rows(tankers: pd.DataFrame, fresh_minutes: int) -> pd.DataFrame:
    """The tankers with marker style and tooltip columns added."""
    if tankers.empty:
        return pd.DataFrame(columns=["longitude", "latitude", "fill", "line", "radius", "line_width", "tooltip"])
    rows = tankers.copy()
    styles = [
        marker_style(row.status, _int_or_none(row.minutes_since_update), fresh_minutes) for row in rows.itertuples()
    ]
    rows["fill"] = [style["fill"] for style in styles]
    rows["line"] = [style["line"] for style in styles]
    rows["radius"] = [style["radius"] for style in styles]
    rows["line_width"] = [2.5 if status == "waiting" else 1 for status in rows["status"]]
    rows["tooltip"] = [
        tooltip_html(
            vessel_name=row.vessel_name,
            status=row.status,
            sog_knots=_float_or_none(row.sog_knots),
            minutes=_int_or_none(row.minutes_since_update),
            flag_country=_str_or_none(row.flag_country),
            length_m=_int_or_none(row.length_m),
            destination=_str_or_none(row.destination),
            zone_name=_str_or_none(row.zone_name),
        )
        for row in rows.itertuples()
    ]
    # pydeck serialises the frame to JSON: keep only what the layer uses.
    return rows[["longitude", "latitude", "fill", "line", "radius", "line_width", "tooltip"]]


def build_deck(
    tankers: pd.DataFrame,
    terminals: pd.DataFrame,
    zones: pd.DataFrame,
    camera: Camera,
    fresh_minutes: int,
    highlighted: str | None = None,
) -> pdk.Deck:
    zone_data = zone_rows(zones, terminals, highlighted, camera.zoom)
    layers = [
        pdk.Layer(
            "PolygonLayer",
            id="zones",
            data=zone_data,
            get_polygon="polygon",
            get_fill_color="fill",
            get_line_color="line",
            line_width_min_pixels=2,
            stroked=True,
            filled=True,
            pickable=False,
        ),
        pdk.Layer(
            "ScatterplotLayer",
            id="tankers",
            data=tanker_rows(tankers, fresh_minutes),
            get_position=["longitude", "latitude"],
            get_fill_color="fill",
            get_line_color="line",
            get_radius="radius",
            get_line_width="line_width",
            radius_units="'pixels'",
            line_width_units="'pixels'",
            stroked=True,
            filled=True,
            pickable=True,
        ),
        pdk.Layer(
            "TextLayer",
            id="zone-labels",
            data=[row for row in zone_data if row["show_label"]],
            get_position="center",
            get_text="label",
            get_size=12,
            get_color=[230, 237, 247, 255],
            get_pixel_offset=[0, -22],
            background=True,
            get_background_color=[11, 18, 32, 225],
            background_padding=[6, 4],
            character_set="'auto'",
            font_family="'Inter, system-ui, sans-serif'",
            pickable=False,
        ),
    ]
    return pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=camera.latitude, longitude=camera.longitude, zoom=camera.zoom),
        map_provider="carto",
        map_style="dark",
        tooltip={"html": "{tooltip}", "style": TOOLTIP_STYLE},
    )
