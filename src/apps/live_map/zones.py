"""Terminal zone geometry: the WKT polygons from live_terminal_zone, as coordinates pydeck can draw."""

import re
from dataclasses import dataclass

_POLYGON = re.compile(r"^\s*POLYGON\s*\(\(\s*(.+?)\s*\)\)\s*$", re.IGNORECASE)


class ZoneShapeError(ValueError):
    """A zone boundary is not a simple WKT polygon."""


@dataclass(frozen=True)
class Bounds:
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    @property
    def center(self) -> tuple[float, float]:
        """(longitude, latitude) of the middle of the box."""
        return (self.min_lon + self.max_lon) / 2, (self.min_lat + self.max_lat) / 2

    @property
    def span_degrees(self) -> float:
        return max(self.max_lon - self.min_lon, self.max_lat - self.min_lat)


def parse_polygon(wkt: str) -> list[list[float]]:
    """The outer ring of 'POLYGON((lon lat, ...))' as [[lon, lat], ...]."""
    match = _POLYGON.match(wkt or "")
    if not match:
        raise ZoneShapeError(f"Not a simple WKT polygon: {wkt!r}")
    ring = []
    for point in match.group(1).split(","):
        parts = point.split()
        if len(parts) != 2:
            raise ZoneShapeError(f"Bad point {point!r} in {wkt!r}")
        ring.append([float(parts[0]), float(parts[1])])
    return ring


def bounds(ring: list[list[float]]) -> Bounds:
    lons = [lon for lon, _ in ring]
    lats = [lat for _, lat in ring]
    return Bounds(min(lons), min(lats), max(lons), max(lats))
