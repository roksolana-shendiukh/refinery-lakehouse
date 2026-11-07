"""Producer settings, read from environment variables. Secrets never live in code or files."""

import os
from dataclasses import dataclass, field

# AIS bounding boxes as [[lat_min, lon_min], [lat_max, lon_max]]
US_COASTS = (
    ((24.0, -98.0), (31.0, -80.0)),
    ((24.5, -82.0), (45.5, -66.0)),
    ((32.0, -125.0), (49.0, -117.0)),
)

MESSAGE_TYPES = ("PositionReport", "ShipStaticData")


class ConfigError(ValueError):
    """A required setting is missing."""


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Environment variable {name} is not set")
    return value


@dataclass(frozen=True)
class ProducerConfig:
    zerobus_endpoint: str
    workspace_url: str
    table_name: str
    client_id: str
    client_secret: str = field(repr=False)
    ais_api_key: str = field(repr=False)
    bounding_boxes: tuple = US_COASTS
    message_types: tuple = MESSAGE_TYPES
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
        )
