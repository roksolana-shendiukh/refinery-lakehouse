import pytest
from views import COAST_CAMERAS, MAX_ZOOM, MIN_ZOOM, coast_camera, terminal_camera, zoomed
from zones import ZoneShapeError, bounds, parse_polygon

HOUSTON = "POLYGON((-95.3 29.6, -94.95 29.6, -94.95 29.82, -95.3 29.82, -95.3 29.6))"


def test_parse_polygon_gives_lon_lat_pairs():
    ring = parse_polygon(HOUSTON)

    assert ring[0] == [-95.3, 29.6]
    assert len(ring) == 5


@pytest.mark.parametrize("wkt", ["", "POINT(1 2)", "POLYGON((1 2 3, 4 5 6))", None])
def test_parse_polygon_rejects_other_shapes(wkt):
    with pytest.raises(ZoneShapeError):
        parse_polygon(wkt)


def test_bounds_center_and_span():
    box = bounds(parse_polygon(HOUSTON))

    assert box.center == pytest.approx((-95.125, 29.71))
    assert box.span_degrees == pytest.approx(0.35)


def test_unknown_coast_falls_back_to_all_coasts():
    assert coast_camera(None) == COAST_CAMERAS["All coasts"]
    assert coast_camera("Arctic") == COAST_CAMERAS["All coasts"]


def test_zoom_stays_within_limits():
    camera = coast_camera("Gulf")

    assert zoomed(camera, 1).zoom == camera.zoom + 1
    assert zoomed(camera, 100).zoom == MAX_ZOOM
    assert zoomed(camera, -100).zoom == MIN_ZOOM
    assert zoomed(camera, 1).latitude == camera.latitude


def test_terminal_camera_centres_on_the_zone():
    camera = terminal_camera(bounds(parse_polygon(HOUSTON)))

    assert (camera.longitude, camera.latitude) == pytest.approx((-95.125, 29.71))
    assert 7 <= camera.zoom <= 11
