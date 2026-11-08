"""Presentation rules for the live map: marker and zone styles, labels, tooltip and freshness text.

Pure functions: no Dash, no data access, no settings of their own (thresholds come as parameters).
Styles are Leaflet path options (color, fillColor, ...) and match the approved design.
"""

import html
from dataclasses import dataclass

STATUS_LABELS = {"underway": "Underway", "waiting": "Waiting", "at_berth": "At berth"}

# Leaflet path options per status. "color" is the outline, "weight" its width in pixels.
STATUS_STYLE = {
    "underway": {"radius": 4, "color": "#0B1220", "weight": 1, "fillColor": "#7CC4FF", "fillOpacity": 0.9},
    "waiting": {"radius": 4, "color": "#FFC266", "weight": 2.5, "fillColor": "#0E1A2E", "fillOpacity": 1.0},
    "at_berth": {"radius": 4, "color": "#0B1220", "weight": 1, "fillColor": "#E6EDF7", "fillOpacity": 1.0},
}
STALE_OPACITY = 0.4

LEVEL_LABELS = {"high": "High", "elevated": "Elevated", "normal": "Normal"}
LEVEL_STYLE = {
    "high": {"color": "#FF8A6B", "weight": 2, "fillColor": "#FF6B4A", "fillOpacity": 0.35},
    "elevated": {"color": "#FFC266", "weight": 2, "fillColor": "#FFC266", "fillOpacity": 0.28},
    "normal": {"color": "#3FB6A8", "weight": 1.5, "fillColor": "#3FB6A8", "fillOpacity": 0.2},
}
LEVEL_TEXT_COLOR = {"high": "#FF8A6B", "elevated": "#FFC266", "normal": "#7FD8C9"}

STATUS_TEXT_COLOR = {"underway": "#7CC4FF", "waiting": "#FFC266", "at_berth": "#E6EDF7"}


def marker_style(status: str, minutes_since_update: int | None, fresh_minutes: int) -> dict:
    """Leaflet options for one tanker; positions older than fresh_minutes are faded."""
    style = dict(STATUS_STYLE.get(status, STATUS_STYLE["underway"]))
    if minutes_since_update is None or minutes_since_update > fresh_minutes:
        style["opacity"] = STALE_OPACITY
        style["fillOpacity"] = min(style["fillOpacity"], STALE_OPACITY)
    return style


def level_style(level: str | None) -> dict:
    return LEVEL_STYLE.get(level or "normal", LEVEL_STYLE["normal"])


def speed_text(sog_knots: float | None) -> str:
    if sog_knots is None:
        return "speed unknown"
    return f"{sog_knots:.1f} kn"


def age_text(minutes: int | None) -> str:
    if minutes is None:
        return "age unknown"
    if minutes == 0:
        return "just now"
    return f"{minutes} min ago"


def tooltip_html(
    vessel_name: str,
    status: str,
    sog_knots: float | None,
    minutes: int | None,
    flag_country: str | None,
    length_m: int | None,
    destination: str | None,
    zone_name: str | None,
) -> str:
    """The tooltip card for one tanker. Every value from AIS is escaped: crews type these fields."""
    color = STATUS_TEXT_COLOR.get(status, STATUS_TEXT_COLOR["underway"])
    details = [f"Flag: {flag_country or 'unknown'}"]
    if length_m:
        details.append(f"{length_m} m")
    lines = [
        f'<div class="tt-head"><b>{html.escape(vessel_name)}</b><span>{age_text(minutes)}</span></div>',
        f'<div style="color:{color}">{STATUS_LABELS.get(status, status)} · {speed_text(sog_knots)}</div>',
        f"<div>{html.escape(' · '.join(details))}</div>",
        f"<div>Destination: {html.escape(destination or 'unknown')}</div>",
    ]
    if zone_name:
        lines.append(f"<div>In zone: {html.escape(zone_name)}</div>")
    return "".join(lines)


@dataclass(frozen=True)
class Freshness:
    text: str
    kind: str  # "live", "stale" or "empty": picks the badge colour


def freshness(seconds_since_fetch: float | None, error: str | None, tanker_count: int) -> Freshness:
    """The header badge: is the data current, and if not, why."""
    if seconds_since_fetch is None:
        return Freshness("No data yet", "empty")
    if seconds_since_fetch < 120:
        age = f"{int(seconds_since_fetch)} s ago"
    else:
        age = f"{int(seconds_since_fetch // 60)} min ago"
    if error:
        return Freshness(f"Refresh failed · data from {age}", "stale")
    if tanker_count == 0:
        return Freshness("Live · no tankers in the last 6 hours", "empty")
    return Freshness(f"Live · {age}", "live")
