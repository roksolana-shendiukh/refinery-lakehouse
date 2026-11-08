import pytest
from views import ALL_COASTS, COAST_CAMERAS, coast_camera
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
    assert coast_camera(None) == COAST_CAMERAS[ALL_COASTS]
    assert coast_camera("Arctic") == COAST_CAMERAS[ALL_COASTS]


def test_camera_viewport_flies_to_the_coast():
    gulf = coast_camera("Gulf")

    assert gulf.viewport() == {"center": [gulf.latitude, gulf.longitude], "zoom": gulf.zoom, "transition": "flyTo"}
