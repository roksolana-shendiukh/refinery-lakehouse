"""Regenerates the live pipeline's reference views from the CSVs in data/reference.

Run after every change to ref_terminal_zone.csv or ref_mid.csv:
    uv run python scripts/generate_live_reference.py
CI fails (tests/test_live_reference.py) when the committed SQL is out of date.
"""

from pathlib import Path

from refinery.live_reference import LIVE_REFERENCE_VIEWS, output_path
from refinery.reference_sql import render_from_csv

REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    for view in LIVE_REFERENCE_VIEWS:
        target = output_path(view, REPO_ROOT)
        target.write_text(render_from_csv(view, REPO_ROOT), encoding="utf-8")
        print(f"Wrote {target.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
