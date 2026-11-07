"""Producer settings, read from environment variables. Secrets never live in code or files."""

import os
from dataclasses import dataclass, field

DEFAULT_AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"

US_COASTS = (
    ((24.0, -82.0), (45.5, -66.0)),  
    ((24.0, -98.0), (31.0, -82.0)), 
    ((32.0, -125.5), (49.0, -117.0)), 
)

MESSAGE_TYPES = ("PositionReport", "ShipStaticData")


class ConfigError(ValueError):
    """A required setting is missing or invalid."""


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Environment variable {name} is not set")
    return value


def _optional(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _optional_int(name: str, default: int) -> int:
    value = _optional(name, str(default))
    try:
        number = int(value)
    except ValueError:
        raise ConfigError(f"Environment variable {name} must be a whole number, got {value!r}") from None
    if number <= 0:
        raise ConfigError(f"Environment variable {name} must be positive, got {number}")
    return number


@dataclass(frozen=True)
class ProducerConfig:
    # Zerobus
    zerobus_endpoint: str
    workspace_url: str
    table_name: str
    client_id: str
    client_secret: str = field(repr=False)
    # AIS source
    ais_api_key: str = field(repr=False)
    ais_url: str = DEFAULT_AISSTREAM_URL
    ping_interval_seconds: int = 20
    ping_timeout_seconds: int = 20
    bounding_boxes: tuple = US_COASTS
    message_types: tuple = MESSAGE_TYPES
    # Monitoring
    heartbeat_seconds: int = 60

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        return cls(
            zerobus_endpoint=_required("ZEROBUS_SERVER_ENDPOINT"),
            workspace_url=_required("DATABRICKS_WORKSPACE_URL"),
            table_name=_required("ZEROBUS_TABLE"),
            client_id=_required("PRODUCER_CLIENT_ID"),
            client_secret=_required("PRODUCER_CLIENT_SECRET"),
            ais_api_key=_required("AISSTREAM_API_KEY"),
            ais_url=_optional("AISSTREAM_URL", DEFAULT_AISSTREAM_URL),
            ping_interval_seconds=_optional_int("AIS_PING_INTERVAL_SECONDS", 20),
            ping_timeout_seconds=_optional_int("AIS_PING_TIMEOUT_SECONDS", 20),
            heartbeat_seconds=_optional_int("HEARTBEAT_SECONDS", 60),
        )
