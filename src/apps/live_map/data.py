"""Reads the current tanker positions from the live view through a SQL warehouse.

One responsibility: data access. A short cache is shared by every viewer of the app, so the warehouse
gets one query per refresh interval however many people watch. If a refresh fails, the last good
result is kept and the error is reported next to it, so the map never goes blank.
"""

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
from databricks import sql
from databricks.sdk.core import Config

log = logging.getLogger(__name__)

COLUMNS = (
    "mmsi",
    "vessel_name",
    "destination",
    "ship_type",
    "length_m",
    "latitude",
    "longitude",
    "sog_knots",
    "cog_degrees",
    "heading_degrees",
    "position_at",
    "minutes_since_update",
)


class QueryError(RuntimeError):
    """The warehouse query failed (warehouse stopped, permissions, network)."""


@dataclass(frozen=True)
class Snapshot:
    tankers: pd.DataFrame
    fetched_at: float | None  # monotonic time of the data shown; None if there has never been data
    error: str | None = None  # set when the latest refresh failed and older (or no) data is shown


def query_for(table_name: str) -> str:
    return f"SELECT {', '.join(COLUMNS)} FROM {table_name} ORDER BY position_at DESC"


def warehouse_query(warehouse_id: str) -> Callable[[str], pd.DataFrame]:
    """A query function bound to the warehouse. Credentials come from the app's service principal."""
    config = Config()

    def run(query: str) -> pd.DataFrame:
        try:
            with (
                sql.connect(
                    server_hostname=config.host,
                    http_path=f"/sql/1.0/warehouses/{warehouse_id}",
                    credentials_provider=lambda: config.authenticate,
                ) as connection,
                connection.cursor() as cursor,
            ):
                cursor.execute(query)
                return cursor.fetchall_arrow().to_pandas()
        except sql.exc.Error as error:
            raise QueryError(str(error)) from error

    return run


class TankerSource:
    def __init__(
        self,
        run_query: Callable[[str], pd.DataFrame],
        table_name: str,
        ttl_seconds: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._run_query = run_query
        self._query = query_for(table_name)
        self._ttl = ttl_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._last = Snapshot(tankers=pd.DataFrame(columns=COLUMNS), fetched_at=None)
        self._attempted_at: float | None = None

    def snapshot(self) -> Snapshot:
        """The latest tankers; queries the warehouse at most once per ttl_seconds for all viewers."""
        with self._lock:
            now = self._clock()
            if self._attempted_at is not None and now - self._attempted_at < self._ttl:
                return self._last
            self._attempted_at = now
            try:
                self._last = Snapshot(tankers=self._run_query(self._query), fetched_at=now)
            except QueryError as error:
                log.warning("Live map refresh failed, showing the last good data: %s", error)
                self._last = Snapshot(tankers=self._last.tankers, fetched_at=self._last.fetched_at, error=str(error))
            return self._last
