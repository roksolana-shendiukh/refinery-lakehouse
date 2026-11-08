"""Map camera: the starting view for each coast filter.

These are layout constants of the map (like its colours), not environment settings.
Zoom levels are Leaflet zoom levels (256-pixel tiles).
"""

from dataclasses import dataclass

ALL_COASTS = "All coasts"
COASTS = (ALL_COASTS, "East", "Gulf", "West")

MIN_ZOOM = 3
MAX_ZOOM = 13


@dataclass(frozen=True)
class Camera:
    latitude: float
    longitude: float
    zoom: float

    def viewport(self) -> dict:
        """The dash-leaflet viewport that flies the map to this camera."""
        return {"center": [self.latitude, self.longitude], "zoom": self.zoom, "transition": "flyTo"}


COAST_CAMERAS = {
    ALL_COASTS: Camera(latitude=37.0, longitude=-97.0, zoom=4.25),
    "East": Camera(latitude=37.0, longitude=-75.5, zoom=5.5),
    "Gulf": Camera(latitude=28.8, longitude=-92.5, zoom=6.25),
    "West": Camera(latitude=40.5, longitude=-121.0, zoom=5.5),
}


def coast_camera(coast: str | None) -> Camera:
    return COAST_CAMERAS.get(coast or ALL_COASTS, COAST_CAMERAS[ALL_COASTS])
