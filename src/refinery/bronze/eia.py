"""EIA API v2: what to request and where to save the responses."""

import csv
import json
import logging
from datetime import UTC, timedelta
from pathlib import Path
from typing import NamedTuple

logger = logging.getLogger(__name__)

BASE_URL = "https://api.eia.gov/v2"
PAGE_SIZE = 5000  # the most rows EIA returns in one response
LOADS_FOLDER = "_loads"


class Dataset(NamedTuple):
    route: str  # EIA API route
    folder: str  # subfolder in the landing volume
    table: str  # bronze table that reads the folder


DATASETS = {
    "supply": Dataset("petroleum/cons/wpsup", "supply", "eia_supply_raw"),
    "retail": Dataset("petroleum/pri/gnd", "price/retail", "eia_price_raw"),
    "spot": Dataset("petroleum/pri/spt", "price/spot", "eia_price_raw"),
}


def data_url(dataset):
    return f"{BASE_URL}/{DATASETS[dataset].route}/data/"


def request_params(products, offset, start=None):
    """Query parameters for one page; the API key is added by the client, never here."""
    params = [("frequency", "weekly"), ("data[0]", "value")]
    params += [("facets[product][]", product) for product in products]
    params += [
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
        ("sort[1][column]", "duoarea"),
        ("sort[1][direction]", "asc"),
        ("sort[2][column]", "product"),
        ("sort[2][direction]", "asc"),
        ("offset", str(offset)),
        ("length", str(PAGE_SIZE)),
    ]
    if start:
        params.append(("start", start))
    return params


def landing_file(root, dataset, load_id, offset):
    return f"{root}/eia/{DATASETS[dataset].folder}/load_id={load_id}/offset={offset}.json"


def marker_file(root, load_id):
    return f"{root}/eia/{LOADS_FOLDER}/load_id={load_id}/marker.json"


def iter_pages(client, url, products, start=None):
    """Yield (offset, rows) for every page until all rows are fetched."""
    offset = 0
    while True:
        body = client.get_json(url, request_params(products, offset, start))
        total = int(body["response"]["total"])
        rows = body["response"]["data"]

        if not rows:
            if offset < total:
                raise ValueError(f"EIA returned an empty page at offset {offset} of {total}")
            return

        yield offset, rows

        offset += len(rows)
        if offset >= total:
            return


def write_page(root, dataset, load_id, offset, rows):
    """Save one page as JSON Lines: one EIA record per line, exactly as received."""
    path = Path(landing_file(root, dataset, load_id, offset))
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(row) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def fetch_dataset(client, root, dataset, load_id, products, start=None):
    """Fetch every page of one dataset into the landing volume; return what was saved."""
    files = 0
    rows = 0
    for offset, page_rows in iter_pages(client, data_url(dataset), products, start):
        write_page(root, dataset, load_id, offset, page_rows)
        files += 1
        rows += len(page_rows)
    logger.info("Fetched %s: %d files, %d rows", dataset, files, rows)
    return {"files": files, "rows": rows}


def make_load_id(started_at):
    """Load id from the start time, in UTC: 20261112T090000Z."""
    return started_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def write_marker(root, load_id, started_at, counts, start=None):
    path = Path(marker_file(root, load_id))
    path.parent.mkdir(parents=True, exist_ok=True)
    marker = {
        "load_id": load_id,
        "started_at": started_at.astimezone(UTC).isoformat(),
        "start": start,
        "datasets": counts,
    }
    path.write_text(json.dumps(marker, indent=2) + "\n", encoding="utf-8")
    return str(path)


def read_reference(reference_dir, name):
    """All rows of one reference CSV as dictionaries."""
    with open(Path(reference_dir) / f"{name}.csv", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def products_for(dataset, reference_dir):
    """EIA product codes to request for one dataset, taken from the reference CSVs."""
    if dataset == "supply":
        return [row["product_bk"] for row in read_reference(reference_dir, "ref_product")]
    return [
        row["eia_product"] for row in read_reference(reference_dir, "ref_eia_product") if row["price_type"] == dataset
    ]


LOOKBACK_WEEKS = 8  # weeks re-read on every run, to pick up EIA revisions


def has_complete_load(root):
    """True once any load has finished: only a finished load writes a marker."""
    return any((Path(root) / "eia" / LOADS_FOLDER).glob("load_id=*/marker.json"))


def start_for(root, today, lookback_weeks=LOOKBACK_WEEKS):
    """None (full history) until a load has finished; afterwards only the last weeks."""
    if not has_complete_load(root):
        return None
    return (today - timedelta(weeks=lookback_weeks)).isoformat()


def run_load(client, root, reference_dir, started_at):
    """One load: every dataset into the landing volume, then the marker last."""
    load_id = make_load_id(started_at)
    start = start_for(root, started_at.astimezone(UTC).date())
    logger.info("Load %s started: %s", load_id, f"since {start}" if start else "full history")
    counts = {}
    for dataset in DATASETS:
        products = products_for(dataset, reference_dir)
        counts[dataset] = fetch_dataset(client, root, dataset, load_id, products, start)
    write_marker(root, load_id, started_at, counts, start)
    return load_id, counts
