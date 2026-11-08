import pytest
from settings import AppSettings, SettingsError

REQUIRED = {
    "DATABRICKS_WAREHOUSE_ID": "abc",
    "LIVE_TANKERS_TABLE": "c.s.live_tankers",
    "LIVE_TERMINALS_TABLE": "c.s.live_terminal_congestion",
    "LIVE_ZONES_TABLE": "c.s.live_terminal_zone",
    "MAP_TILE_URL": "https://tiles.example/{z}/{x}/{y}.png?key={api_key}",
    "MAP_TILE_KEY": "test-key",
    "MAP_TILE_ATTRIBUTION": "© Example",
}


@pytest.fixture
def env(monkeypatch):
    for name in ("REFRESH_SECONDS", "ZONES_REFRESH_SECONDS", "FRESH_MINUTES"):
        monkeypatch.delenv(name, raising=False)
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


def test_reads_tables_and_defaults(env):
    settings = AppSettings.from_env()

    assert settings.terminals_table == "c.s.live_terminal_congestion"
    assert settings.zones_table == "c.s.live_terminal_zone"
    assert (settings.refresh_seconds, settings.zones_refresh_seconds, settings.fresh_minutes) == (15, 3600, 10)


@pytest.mark.parametrize("name", list(REQUIRED))
def test_missing_required_setting_is_an_error(env, name):
    env.delenv(name)

    with pytest.raises(SettingsError, match=name):
        AppSettings.from_env()


@pytest.mark.parametrize("value", ["0", "-5", "fast"])
def test_refresh_must_be_a_positive_whole_number(env, value):
    env.setenv("REFRESH_SECONDS", value)

    with pytest.raises(SettingsError, match="REFRESH_SECONDS"):
        AppSettings.from_env()


def test_tile_key_is_filled_into_the_url_and_kept_out_of_repr(env):
    settings = AppSettings.from_env()

    assert settings.tile_url == "https://tiles.example/{z}/{x}/{y}.png?key=test-key"
    assert "test-key" not in repr(settings)


def test_tile_url_without_the_key_placeholder_is_an_error(env):
    env.setenv("MAP_TILE_URL", "https://tiles.example/{z}/{x}/{y}.png")

    with pytest.raises(SettingsError, match="api_key"):
        AppSettings.from_env()
