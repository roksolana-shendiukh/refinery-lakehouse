"""Live map settings, read from environment variables set in app.yaml. No values are hardcoded elsewhere."""

import os
from dataclasses import dataclass


class SettingsError(ValueError):
    """A required setting is missing or invalid."""


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SettingsError(f"Environment variable {name} is not set")
    return value


def _positive_int(name: str, default: int) -> int:
    value = os.environ.get(name, "").strip() or str(default)
    try:
        number = int(value)
    except ValueError:
        raise SettingsError(f"Environment variable {name} must be a whole number, got {value!r}") from None
    if number <= 0:
        raise SettingsError(f"Environment variable {name} must be positive, got {number}")
    return number


@dataclass(frozen=True)
class AppSettings:
    warehouse_id: str
    tankers_table: str
    terminals_table: str
    zones_table: str
    refresh_seconds: int = 15
    zones_refresh_seconds: int = 3600
    fresh_minutes: int = 10
    tile_url: str = ""
    tile_attribution: str = ""

    @classmethod
    def from_env(cls) -> "AppSettings":
        return cls(
            warehouse_id=_required("DATABRICKS_WAREHOUSE_ID"),
            tankers_table=_required("LIVE_TANKERS_TABLE"),
            terminals_table=_required("LIVE_TERMINALS_TABLE"),
            zones_table=_required("LIVE_ZONES_TABLE"),
            refresh_seconds=_positive_int("REFRESH_SECONDS", 15),
            zones_refresh_seconds=_positive_int("ZONES_REFRESH_SECONDS", 3600),
            fresh_minutes=_positive_int("FRESH_MINUTES", 10),
            tile_url=_required("MAP_TILE_URL"),
            tile_attribution=_required("MAP_TILE_ATTRIBUTION"),
        )
