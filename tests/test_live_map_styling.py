from styling import FRESH_COLOR, STALE_COLOR, dot_color, freshness_line, speed_text, tooltip_text


def test_recent_positions_are_green_and_old_ones_grey():
    assert dot_color(3, fresh_minutes=10) == FRESH_COLOR
    assert dot_color(11, fresh_minutes=10) == STALE_COLOR
    assert dot_color(None, fresh_minutes=10) == STALE_COLOR


def test_speed_text():
    assert speed_text(None) == "speed unknown"
    assert speed_text(0.1) == "stopped"
    assert speed_text(14.25) == "14.2 kn"


def test_tooltip_text():
    assert tooltip_text("SANMAR RAGA", "PACTB", 14.3, 0) == "SANMAR RAGA → PACTB · 14.3 kn · 0 min ago"
    assert tooltip_text("TAHOE SPIRIT", None, 0.0, None) == "TAHOE SPIRIT → destination unknown · stopped · age unknown"


def test_freshness_line():
    assert freshness_line(2, 6) == "Last position update: 2 min ago · tankers shown: 6"
    assert freshness_line(None, 0).startswith("No tankers")
