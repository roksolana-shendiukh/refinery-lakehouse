"""Assistant settings, read from environment variables (set in app.yaml for the app)."""

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
class AssistantSettings:
    llm_endpoint: str  # chat model serving endpoint that answers and calls tools
    index_name: str  # vector search index over the EIA article chunks
    log_table: str  # request log, one row per question
    warehouse_id: str  # SQL warehouse that writes the request log
    search_results: int = 5  # chunks returned by one search
    max_iterations: int = 3  # model calls per question, so a looping model cannot run up cost

    @classmethod
    def from_env(cls) -> "AssistantSettings":
        return cls(
            llm_endpoint=_required("ASSISTANT_LLM_ENDPOINT"),
            index_name=_required("ASSISTANT_INDEX_NAME"),
            log_table=_required("ASSISTANT_LOG_TABLE"),
            warehouse_id=_required("DATABRICKS_WAREHOUSE_ID"),
            search_results=_positive_int("ASSISTANT_SEARCH_RESULTS", 5),
            max_iterations=_positive_int("ASSISTANT_MAX_ITERATIONS", 3),
        )
