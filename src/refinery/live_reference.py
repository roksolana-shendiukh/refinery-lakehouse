"""Which reference CSVs the live pipeline uses, and where their generated SQL lives."""

from pathlib import Path

from refinery.reference_sql import ReferenceView

LIVE_SQL_DIR = "src/refinery/live"

LIVE_REFERENCE_VIEWS = (
    ReferenceView(
        view_name="live_ref_terminal_zone",
        comment="Terminal zones (polygons, WKT) from data/reference/ref_terminal_zone.csv, for the live map",
        source_csv="data/reference/ref_terminal_zone.csv",
        column_types={
            "port_bk": "STRING",
            "port_name": "STRING",
            "terminal_type": "STRING",
            "area_bk": "STRING",
            "boundary_wkt": "STRING",
        },
    ),
    ReferenceView(
        view_name="live_ref_mid",
        comment="Maritime Identification Digits (first 3 digits of an MMSI) to flag country, for the live map",
        source_csv="data/reference/ref_mid.csv",
        column_types={
            "mid": "SMALLINT",
            "country_name": "STRING",
        },
    ),
)


def output_path(view: ReferenceView, repo_root: Path) -> Path:
    return repo_root / LIVE_SQL_DIR / f"{view.view_name}.sql"
