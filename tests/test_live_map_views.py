import json

import pandas as pd
from map_view import build_deck, tanker_rows, zone_rows
from summary import summarize
from terminals_view import table_frame
from views import coast_camera

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
    assert (summary.most_congested, summary.most_congested_waiting) == ("Houston Ship Channel", 9)


def test_summary_without_waiting_tankers_has_no_most_congested():
    quiet = TERMINALS.assign(waiting=0)

    assert summarize(TANKERS.iloc[0:0], quiet).most_congested is None


def test_zone_rows_skip_bad_shapes_and_colour_by_level():
    rows = {row["port_bk"]: row for row in zone_rows(ZONES, TERMINALS, highlighted=None, zoom=3.4)}

    assert set(rows) == {"HOU", "SAB", "BOS"}
    assert rows["HOU"]["show_label"] and rows["HOU"]["label"] == "Houston Ship Channel · 9 waiting"
    assert not rows["SAB"]["show_label"]  # elevated zones are labelled only when zoomed in
    assert not rows["BOS"]["show_label"]


def test_zone_rows_label_elevated_when_zoomed_in_and_the_highlighted_terminal():
    rows = {row["port_bk"]: row for row in zone_rows(ZONES, TERMINALS, highlighted="BOS", zoom=6)}

    assert rows["SAB"]["show_label"]
    assert rows["BOS"]["show_label"]


def test_tanker_rows_keep_only_layer_columns():
    rows = tanker_rows(TANKERS, fresh_minutes=10)

    assert list(rows.columns) == ["longitude", "latitude", "fill", "line", "radius", "line_width", "tooltip"]
    assert rows["line_width"].tolist() == [2.5, 2.5, 1, 1]


def test_deck_uses_pixel_units_and_the_camera():
    deck = json.loads(build_deck(TANKERS, TERMINALS, ZONES, coast_camera("Gulf"), fresh_minutes=10).to_json())
    tankers = next(layer for layer in deck["layers"] if layer["id"] == "tankers")

    # A plain string would be read as a column name ("@@=pixels"), and markers would scale with zoom.
    assert tankers["radiusUnits"] == "pixels"
    assert deck["initialViewState"]["zoom"] == coast_camera("Gulf").zoom


def test_deck_with_no_data_still_builds():
    deck = build_deck(TANKERS.iloc[0:0], TERMINALS.iloc[0:0], ZONES.iloc[0:0], coast_camera(None), fresh_minutes=10)

    assert len(deck.layers) == 3


def test_terminal_table_is_ranked_and_filtered_by_coast():
    frame = table_frame(TERMINALS, "All coasts")

    assert frame["port_bk"].tolist() == ["HOU", "SAB", "BOS"]
    assert frame.loc[0, "coast_label"] == "Gulf · PADD 3"
    assert frame.loc[0, "level"] == "High"
    assert table_frame(TERMINALS, "East")["port_bk"].tolist() == ["BOS"]
