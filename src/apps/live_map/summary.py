"""The key figures over the map: tanker counts by status and the most congested terminal."""

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class Summary:
    tankers: int
    underway: int
    waiting: int
    at_berth: int
    most_congested: str | None  # terminal name; None when no terminal has a tanker waiting
    most_congested_waiting: int


def summarize(tankers: pd.DataFrame, terminals: pd.DataFrame) -> Summary:
    counts = tankers["status"].value_counts() if not tankers.empty else pd.Series(dtype=int)
    busiest, busiest_waiting = None, 0
    if not terminals.empty:
        top = terminals.sort_values(["waiting", "at_berth"], ascending=False).iloc[0]
        if int(top["waiting"]) > 0:
            busiest, busiest_waiting = str(top["port_name"]), int(top["waiting"])
    return Summary(
        tankers=len(tankers),
        underway=int(counts.get("underway", 0)),
        waiting=int(counts.get("waiting", 0)),
        at_berth=int(counts.get("at_berth", 0)),
        most_congested=busiest,
        most_congested_waiting=busiest_waiting,
    )
