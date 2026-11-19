from styling import STALE_OPACITY, STATUS_STYLE, freshness, level_style, marker_style, speed_text, tooltip_html


def test_recent_positions_keep_the_status_style():
    assert marker_style("waiting", 3, fresh_minutes=10) == STATUS_STYLE["waiting"]


def test_old_or_unknown_positions_are_faded():
    for minutes in (11, None):
        style = marker_style("underway", minutes, fresh_minutes=10)
        assert style["opacity"] == STALE_OPACITY
        assert style["fillOpacity"] == STALE_OPACITY
        assert style["fillColor"] == STATUS_STYLE["underway"]["fillColor"]


def test_marker_style_does_not_change_the_shared_style():
    marker_style("underway", None, fresh_minutes=10)

    assert "opacity" not in STATUS_STYLE["underway"]


def test_unknown_status_and_level_fall_back_to_defaults():
    assert marker_style("drifting", 0, fresh_minutes=10) == STATUS_STYLE["underway"]
    assert level_style(None) == level_style("normal")


def test_speed_text():
    assert speed_text(None) == "speed unknown"
    assert speed_text(14.25) == "14.2 kn"


def test_tooltip_escapes_ais_text():
    tip = tooltip_html("<b>X</b>", "waiting", 0.2, 1, "Marshall Islands", 228, "US<HOU>", "Houston Ship Channel")

    assert "&lt;b&gt;X&lt;/b&gt;" in tip
    assert "US&lt;HOU&gt;" in tip
    assert "Waiting · 0.2 kn" in tip
    assert "Flag: Marshall Islands · 228 m" in tip
    assert "In zone: Houston Ship Channel" in tip


def test_tooltip_without_optional_fields():
    tip = tooltip_html("TAHOE SPIRIT", "underway", None, None, None, None, None, None)

    assert "Flag: unknown" in tip
    assert "Destination: unknown" in tip
    assert "In zone" not in tip


def test_freshness_badge():
    assert freshness(None, None, 0).kind == "empty"
    assert freshness(12, None, 30).text == "Live · 12 s ago"
    assert freshness(12, None, 0).kind == "empty"
    stale = freshness(300, "warehouse stopped", 30)
    assert (stale.kind, stale.text) == ("stale", "Refresh failed · data from 5 min ago")
