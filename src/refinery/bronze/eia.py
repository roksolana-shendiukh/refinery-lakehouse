"""EIA API v2: what to request and where to save the responses."""

import json
from datetime import UTC
from pathlib import Path
from typing import NamedTuple

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
