"""Map camera: the view for each coast, zoom steps, and the view that frames one terminal zone.

These are layout constants of the map (like its colours), not environment settings.
"""

import math
from dataclasses import dataclass, replace

from zones import Bounds

MIN_ZOOM = 2.0
MAX_ZOOM = 13.0
ZOOM_STEP = 1.0
TERMINAL_MIN_ZOOM = 7.0
TERMINAL_MAX_ZOOM = 11.0

ALL_COASTS = "All coasts"


@dataclass(frozen=True)
class Camera:
    latitude: float
    longitude: float
    zoom: float


COAST_CAMERAS = {
    ALL_COASTS: Camera(latitude=36.0, longitude=-97.0, zoom=3.4),
    "East": Camera(latitude=36.5, longitude=-75.5, zoom=4.6),
    "Gulf": Camera(latitude=28.8, longitude=-92.5, zoom=5.3),
    "West": Camera(latitude=40.5, longitude=-121.0, zoom=4.5),
}


def coast_camera(coast: str | None) -> Camera:
    return COAST_CAMERAS.get(coast or ALL_COASTS, COAST_CAMERAS[ALL_COASTS])


def zoomed(camera: Camera, steps: float) -> Camera:
    """The same centre, zoomed in (positive steps) or out (negative), kept within MIN_ZOOM..MAX_ZOOM."""
    zoom = min(MAX_ZOOM, max(MIN_ZOOM, camera.zoom + steps * ZOOM_STEP))
    return replace(camera, zoom=zoom)


def terminal_camera(zone: Bounds) -> Camera:
    """Centred on the zone, zoomed so the whole zone and its approaches are in view."""
    longitude, latitude = zone.center
    zoom = math.log2(360 / max(zone.span_degrees, 0.01)) - 1.5
    return Camera(latitude=latitude, longitude=longitude, zoom=min(TERMINAL_MAX_ZOOM, max(TERMINAL_MIN_ZOOM, zoom)))
