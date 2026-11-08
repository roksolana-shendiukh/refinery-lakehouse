"""Presentation rules for the live map: dot colour, tooltip text, freshness line.

Pure functions: no Streamlit, no data access, no settings of their own (thresholds come as parameters).
"""

# pydeck colours as [red, green, blue, alpha]
FRESH_COLOR = [34, 139, 34, 220]
STALE_COLOR = [128, 128, 128, 160]


def dot_color(minutes_since_update: int | None, fresh_minutes: int) -> list[int]:
    """Green while the position is recent, grey once it is older than fresh_minutes or unknown."""
    if minutes_since_update is None or minutes_since_update > fresh_minutes:
        return STALE_COLOR
    return FRESH_COLOR


def speed_text(sog_knots: float | None) -> str:
    if sog_knots is None:
        return "speed unknown"
    if sog_knots < 0.5:
        return "stopped"
    return f"{sog_knots:.1f} kn"


def tooltip_text(vessel_name: str, destination: str | None, sog_knots: float | None, minutes: int | None) -> str:
    """One line per tanker for the map tooltip."""
    where = destination or "destination unknown"
    age = "age unknown" if minutes is None else f"{minutes} min ago"
    return f"{vessel_name} → {where} · {speed_text(sog_knots)} · {age}"


def freshness_line(newest_minutes: int | None, tanker_count: int) -> str:
    """The header line: is the feed alive, and how many tankers are on the map."""
    if tanker_count == 0 or newest_minutes is None:
        return "No tankers with a position in the last 6 hours: is the producer and the pipeline running?"
    return f"Last position update: {newest_minutes} min ago · tankers shown: {tanker_count}"
