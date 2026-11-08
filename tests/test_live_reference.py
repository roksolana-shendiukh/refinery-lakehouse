"""The live pipeline's reference views must match the CSVs in data/reference."""

import csv
from pathlib import Path

import pytest

from refinery.live_reference import LIVE_REFERENCE_VIEWS, output_path
from refinery.reference_sql import render_from_csv

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.parametrize("view", LIVE_REFERENCE_VIEWS, ids=lambda view: view.view_name)
def test_generated_sql_is_up_to_date(view):
    committed = output_path(view, REPO_ROOT).read_text(encoding="utf-8")

    assert committed == render_from_csv(view, REPO_ROOT), (
        f"{view.view_name}.sql is out of date: run `uv run python scripts/generate_live_reference.py`"
    )


def test_terminal_zone_areas_map_to_a_coast():
    with (REPO_ROOT / "data/reference/ref_terminal_zone.csv").open(encoding="utf-8") as file:
        padds = {row["area_bk"][1] for row in csv.DictReader(file)}

    # live_terminal_zone.sql labels only PADD 1, 3 and 5; a zone elsewhere would get no coast.
    assert padds <= {"1", "3", "5"}
