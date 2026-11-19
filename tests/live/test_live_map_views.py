import pandas as pd
from callbacks import HIDDEN_PANE, clicked_port, fit_viewport, pane_classes
from layout import TERMINALS_TAB, chips, most_congested_text
from map_layers import tanker_markers, zone_bounds, zone_shapes
from summary import summarize
from terminals_table import MIN_BAR_PERCENT, bar_percent, table, table_frame

TERMINALS = pd.DataFrame(
    [
        {
            "port_bk": "HOU",
            "port_name": "Houston Ship Channel",
            "coast": "Gulf",
            "padd": "PADD 3",
            "waiting": 9,
            "at_berth": 0,
            "inbound": 1,
            "congestion_level": "high",
        },
        {
            "port_bk": "SAB",
            "port_name": "Sabine Pass approach",
            "coast": "Gulf",
            "padd": "PADD 3",
            "waiting": 4,
            "at_berth": 1,
            "inbound": 0,
            "congestion_level": "elevated",
        },
        {
            "port_bk": "BOS",
            "port_name": "Boston Harbor",
            "coast": "East",
            "padd": "PADD 1",
            "waiting": 0,
            "at_berth": 2,
            "inbound": 0,
            "congestion_level": "normal",
        },
    ]
)
ZONES = pd.DataFrame(
    [
        {"port_bk": "HOU", "boundary_wkt": "POLYGON((-95.3 29.6, -94.95 29.6, -94.95 29.82, -95.3 29.82, -95.3 29.6))"},
        {
            "port_bk": "SAB",
            "boundary_wkt": "POLYGON((-93.95 29.4, -93.45 29.4, -93.45 29.82, -93.95 29.82, -93.95 29.4))",
        },
        {"port_bk": "BOS", "boundary_wkt": "POLYGON((-71.1 42.2, -70.85 42.2, -70.85 42.45, -71.1 42.45, -71.1 42.2))"},
        {"port_bk": "BAD", "boundary_wkt": "POINT(1 2)"},
    ]
)


def tanker(mmsi, status, minutes=0, **extra):
    row = {
        "mmsi": mmsi,
        "vessel_name": "SANMAR RAGA",
        "destination": "USHOU",
        "flag_country": "Liberia",
        "length_m": 228,
        "latitude": 29.7,
        "longitude": -95.1,
        "sog_knots": 0.2,
        "cog_degrees": 0.0,
        "status": status,
        "zone_name": None,
        "position_at": None,
        "minutes_since_update": minutes,
    }
    row.update(extra)
    return row


TANKERS = pd.DataFrame([tanker(1, "waiting"), tanker(2, "waiting"), tanker(3, "underway"), tanker(4, "at_berth")])


def test_summary_counts_statuses_and_finds_the_busiest_terminal():
    summary = summarize(TANKERS, TERMINALS)

    assert (summary.tankers, summary.underway, summary.waiting, summary.at_berth) == (4, 1, 2, 1)
    assert most_congested_text(summary) == "Most congested: Houston Ship Channel · 9 waiting"
    assert len(chips(summary)) == 4


def test_summary_without_waiting_tankers_has_no_most_congested():
    summary = summarize(TANKERS.iloc[0:0], TERMINALS.assign(waiting=0))

    assert summary.most_congested is None
    assert most_congested_text(summary) == ""


def test_markers_have_stable_ids_so_a_refresh_updates_them_in_place():
    markers = tanker_markers(TANKERS, fresh_minutes=10)

    assert [marker.id for marker in markers] == ["tanker-1", "tanker-2", "tanker-3", "tanker-4"]
    assert markers[0].center == [29.7, -95.1]


def test_markers_skip_rows_without_a_position():
    rows = pd.DataFrame([tanker(1, "underway", latitude=None)])

    assert tanker_markers(rows, fresh_minutes=10) == []


def test_zone_shapes_skip_bad_boundaries_and_use_lat_lon_order():
    shapes = {shape.id: shape for shape in zone_shapes(ZONES, TERMINALS, zoom=4.25, highlighted=None)}

    assert set(shapes) == {"zone-HOU", "zone-SAB", "zone-BOS"}
    assert shapes["zone-HOU"].positions[0] == [29.6, -95.3]


def test_zone_labels_show_high_always_and_elevated_only_when_zoomed_in():
    def permanent(zoom, highlighted=None):
        return {s.id: s.children[0].permanent for s in zone_shapes(ZONES, TERMINALS, zoom, highlighted)}

    assert permanent(4.25) == {"zone-HOU": True, "zone-SAB": False, "zone-BOS": False}
    assert permanent(7) == {"zone-HOU": True, "zone-SAB": True, "zone-BOS": False}
    assert permanent(4.25, highlighted="BOS")["zone-BOS"] is True


def test_zone_bounds_for_fitting_the_map():
    assert zone_bounds(ZONES, "HOU") == [[29.6, -95.3], [29.82, -94.95]]
    assert zone_bounds(ZONES, "BAD") is None
    assert zone_bounds(ZONES, "XXX") is None


def test_terminal_table_is_ranked_and_filtered_by_coast():
    assert table_frame(TERMINALS, "All coasts")["port_bk"].tolist() == ["HOU", "SAB", "BOS"]
    assert table_frame(TERMINALS, "East")["port_bk"].tolist() == ["BOS"]


def test_terminal_table_rows_are_clickable_by_port():
    rows = table(table_frame(TERMINALS, None)).children[1].children

    assert [row.id["port"] for row in rows] == ["HOU", "SAB", "BOS"]


def test_empty_terminal_table_says_so():
    body = table(TERMINALS.iloc[0:0]).children[1]

    assert "No terminal data yet" in str(body)


def test_bar_percent():
    assert bar_percent(9, 9) == 100
    assert bar_percent(0, 9) == MIN_BAR_PERCENT
    assert bar_percent(0, 0) == MIN_BAR_PERCENT


def test_only_the_selected_pane_is_visible():
    map_class, terminals_class = pane_classes(TERMINALS_TAB)

    assert map_class == HIDDEN_PANE
    assert "pane-hidden" not in terminals_class
    assert "pane-hidden" in pane_classes("Map")[1]


def test_clicked_port_ignores_table_redraws():
    row = {"type": "terminal-row", "port": "HOU"}

    assert clicked_port(row, 1) == "HOU"
    assert clicked_port(row, 0) is None
    assert clicked_port("reset-view", 1) is None


def test_fit_viewport_flies_to_the_zone():
    viewport = fit_viewport([[29.6, -95.3], [29.82, -94.95]])

    assert viewport["transition"] == "flyToBounds"
    assert viewport["bounds"] == [[29.6, -95.3], [29.82, -94.95]]
