"""Builds the map layers as dash-leaflet components: terminal zones and tankers.

Pure: data frames in, components out. Each component has a stable id (port code, MMSI), so a refresh
updates the existing markers in place instead of redrawing the map.
"""

import dash_leaflet as dl
import pandas as pd
from styling import level_style, marker_style, tooltip_html
from zones import ZoneShapeError, bounds, parse_polygon

ELEVATED_LABEL_MIN_ZOOM = 6  # below this, only high-congestion zones are labelled, so labels do not overlap


def _int_or_none(value) -> int | None:
    return None if pd.isna(value) else int(value)


def _float_or_none(value) -> float | None:
    return None if pd.isna(value) else float(value)


def _str_or_none(value) -> str | None:
    return None if pd.isna(value) or value == "" else str(value)


def zone_bounds(zones: pd.DataFrame, port_bk: str) -> list[list[float]] | None:
    """[[south, west], [north, east]] of one zone, for the map to fit; None if unknown."""
    match = zones[zones["port_bk"] == port_bk]
    if match.empty:
        return None
    try:
        box = bounds(parse_polygon(match.iloc[0]["boundary_wkt"]))
    except ZoneShapeError:
        return None
    return [[box.min_lat, box.min_lon], [box.max_lat, box.max_lon]]


def show_zone_label(level: str, zoom: float | None, highlighted: bool) -> bool:
    if highlighted or level == "high":
        return True
    return level == "elevated" and (zoom or 0) >= ELEVATED_LABEL_MIN_ZOOM


def zone_shapes(
    zones: pd.DataFrame, terminals: pd.DataFrame, zoom: float | None, highlighted: str | None
) -> list[dl.Polygon]:
    """One polygon per zone with a valid boundary, coloured by congestion; busy zones get a label."""
    by_port = terminals.set_index("port_bk").to_dict("index") if not terminals.empty else {}
    shapes = []
    for zone in zones.itertuples():
        try:
            ring = parse_polygon(zone.boundary_wkt)
        except ZoneShapeError:
            continue
        terminal = by_port.get(zone.port_bk, {})
        level = terminal.get("congestion_level") or "normal"
        name = terminal.get("port_name", zone.port_bk)
        waiting = int(terminal.get("waiting") or 0)
        label = f"{name} · {waiting} waiting"
        children = [
            dl.Tooltip(
                content=label,
                permanent=show_zone_label(level, zoom, zone.port_bk == highlighted),
                direction="top",
                className="zone-label",
            )
        ]
        shapes.append(
            dl.Polygon(
                id=f"zone-{zone.port_bk}",
                positions=[[lat, lon] for lon, lat in ring],
                children=children,
                **level_style(level),
            )
        )
    return shapes


def tanker_markers(tankers: pd.DataFrame, fresh_minutes: int) -> list[dl.CircleMarker]:
    markers = []
    for row in tankers.itertuples():
        if pd.isna(row.latitude) or pd.isna(row.longitude):
            continue
        minutes = _int_or_none(row.minutes_since_update)
        tip = tooltip_html(
            vessel_name=row.vessel_name,
            status=row.status,
            sog_knots=_float_or_none(row.sog_knots),
            minutes=minutes,
            flag_country=_str_or_none(row.flag_country),
            length_m=_int_or_none(row.length_m),
            destination=_str_or_none(row.destination),
            zone_name=_str_or_none(row.zone_name),
        )
        markers.append(
            dl.CircleMarker(
                id=f"tanker-{row.mmsi}",
                center=[float(row.latitude), float(row.longitude)],
                children=[dl.Tooltip(content=tip, className="tanker-tip", direction="top")],
                **marker_style(row.status, minutes, fresh_minutes),
            )
        )
    return markers
