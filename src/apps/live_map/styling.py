"""Presentation rules for the live map: colours, labels, tooltip and freshness text.

Pure functions: no Streamlit, no data access, no settings of their own (thresholds come as parameters).
Colours are pydeck [red, green, blue, alpha] lists; they match the approved design.
"""

import html
from dataclasses import dataclass

STATUS_LABELS = {"underway": "Underway", "waiting": "Waiting", "at_berth": "At berth"}

# Marker per status: fill, outline, radius in pixels.
STATUS_STYLE = {
    "underway": {"fill": [124, 196, 255, 230], "line": [11, 18, 32, 255], "radius": 4},
    "waiting": {"fill": [14, 26, 46, 255], "line": [255, 194, 102, 255], "radius": 5},
    "at_berth": {"fill": [230, 237, 247, 255], "line": [11, 18, 32, 255], "radius": 4},
}
STALE_ALPHA = 110

LEVEL_LABELS = {"high": "High", "elevated": "Elevated", "normal": "Normal"}
LEVEL_STYLE = {
    "high": {"fill": [255, 107, 74, 90], "line": [255, 138, 107, 255]},
    "elevated": {"fill": [255, 194, 102, 72], "line": [255, 194, 102, 255]},
    "normal": {"fill": [63, 182, 168, 50], "line": [63, 182, 168, 200]},
}

STATUS_TEXT_COLOR = {"underway": "#7CC4FF", "waiting": "#FFC266", "at_berth": "#E6EDF7"}


def marker_style(status: str, minutes_since_update: int | None, fresh_minutes: int) -> dict:
    """Fill, outline and radius for one tanker; positions older than fresh_minutes are faded."""
    style = STATUS_STYLE.get(status, STATUS_STYLE["underway"])
    stale = minutes_since_update is None or minutes_since_update > fresh_minutes
    if not stale:
        return style
    return {
        "fill": style["fill"][:3] + [min(style["fill"][3], STALE_ALPHA)],
        "line": style["line"][:3] + [STALE_ALPHA],
        "radius": style["radius"],
    }


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
    age = (
        f"{int(seconds_since_fetch)} s ago"
        if seconds_since_fetch < 120
        else f"{int(seconds_since_fetch // 60)} min ago"
    )
    if error:
        return Freshness(f"Refresh failed · data from {age}", "stale")
    if tanker_count == 0:
        return Freshness("Live · no tankers in the last 6 hours", "empty")
    return Freshness(f"Live · {age}", "live")
