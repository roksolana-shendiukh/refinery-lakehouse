import json
import re
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from refinery.bronze.eia import (
    DATASETS,
    PAGE_SIZE,
    data_url,
    fetch_dataset,
    iter_pages,
    landing_file,
    make_load_id,
    marker_file,
    products_for,
    request_params,
    run_load,
    start_for,
    write_marker,
    write_page,
)

LOAD_ID = "20261112T090000Z"
URL = "https://api.eia.gov/v2/petroleum/cons/wpsup/data/"


class FakeClient:
    def __init__(self, pages):
        self.pages = pages
        self.requests = []

    def get_json(self, url, params):
        self.requests.append((url, params))
        offset = int(dict(params)["offset"])
        return self.pages[offset]


def page(rows, total):
    return {"response": {"total": str(total), "data": rows}}


def rows(count):
    return [{"value": str(i)} for i in range(count)]


def test_data_url_for_supply():
    assert data_url("supply") == "https://api.eia.gov/v2/petroleum/cons/wpsup/data/"


def test_both_price_datasets_go_to_one_bronze_table():
    assert DATASETS["retail"].table == DATASETS["spot"].table == "eia_price_raw"


def test_price_folders_share_one_parent_folder():
    assert DATASETS["retail"].folder.startswith("price/")
    assert DATASETS["spot"].folder.startswith("price/")


def test_request_params_has_one_facet_per_product():
    params = request_params(["EPM0F", "EPD0"], offset=0)

    products = [value for key, value in params if key == "facets[product][]"]

    assert products == ["EPM0F", "EPD0"]


def test_request_params_asks_for_one_weekly_page():
    params = dict(request_params(["EPM0F"], offset=5000))

    assert params["frequency"] == "weekly"
    assert params["data[0]"] == "value"
    assert params["offset"] == "5000"
    assert params["length"] == str(PAGE_SIZE)


def test_request_params_without_start_takes_full_history():
    params = dict(request_params(["EPM0F"], offset=0))

    assert "start" not in params


def test_request_params_with_start():
    params = dict(request_params(["EPM0F"], offset=0, start="2026-09-01"))

    assert params["start"] == "2026-09-01"


def test_request_params_never_contain_api_key():
    params = request_params(["EPM0F"], offset=0, start="2026-09-01")

    assert all("api_key" not in key for key, _ in params)


def test_landing_file_path_has_parts_bronze_extracts():
    path = landing_file("/Volumes/raw", "retail", LOAD_ID, 5000)

    assert path == f"/Volumes/raw/eia/price/retail/load_id={LOAD_ID}/offset=5000.json"
    assert re.search(r"load_id=([^/]+)", path).group(1) == LOAD_ID


def test_marker_file_lives_in_loads_folder():
    path = marker_file("/Volumes/raw", LOAD_ID)

    assert path == f"/Volumes/raw/eia/_loads/load_id={LOAD_ID}/marker.json"


def test_iter_pages_returns_one_page_when_all_rows_fit():
    client = FakeClient({0: page(rows(3), total=3)})

    pages = list(iter_pages(client, URL, ["EPM0F"]))

    assert [(offset, len(data)) for offset, data in pages] == [(0, 3)]
    assert len(client.requests) == 1


def test_iter_pages_follows_rows_received_until_total():
    client = FakeClient(
        {
            0: page(rows(5), total=12),
            5: page(rows(5), total=12),
            10: page(rows(2), total=12),
        }
    )

    pages = list(iter_pages(client, URL, ["EPM0F"]))

    assert [offset for offset, _ in pages] == [0, 5, 10]


def test_iter_pages_returns_nothing_when_there_is_no_data():
    client = FakeClient({0: page([], total=0)})

    pages = list(iter_pages(client, URL, ["EPM0F"]))

    assert pages == []


def test_iter_pages_fails_on_empty_page_before_total():
    client = FakeClient(
        {
            0: page(rows(5), total=12),
            5: page([], total=12),
        }
    )

    with pytest.raises(ValueError):
        list(iter_pages(client, URL, ["EPM0F"]))


def test_iter_pages_sends_url_products_and_start_with_every_request():
    client = FakeClient(
        {
            0: page(rows(5), total=7),
            5: page(rows(2), total=7),
        }
    )

    list(iter_pages(client, URL, ["EPM0F", "EPD0"], start="2026-09-01"))

    for url, params in client.requests:
        assert url == URL
        assert [v for k, v in params if k == "facets[product][]"] == ["EPM0F", "EPD0"]
        assert dict(params)["start"] == "2026-09-01"


def test_write_page_saves_one_record_per_line(tmp_path):
    records = [
        {"period": "2026-10-31", "value": "8945"},
        {"period": "2026-10-24", "value": "8890"},
    ]

    path = write_page(str(tmp_path), "supply", LOAD_ID, 0, records)

    assert path == landing_file(str(tmp_path), "supply", LOAD_ID, 0)
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == records


def test_write_page_keeps_values_exactly_as_received(tmp_path):
    records = [{"value": "8945"}, {"value": None}]

    path = write_page(str(tmp_path), "supply", LOAD_ID, 0, records)

    lines = Path(path).read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["value"] for line in lines] == ["8945", None]


def test_fetch_dataset_saves_every_page_and_counts_rows(tmp_path):
    client = FakeClient(
        {
            0: page(rows(5), total=7),
            5: page(rows(2), total=7),
        }
    )

    counts = fetch_dataset(client, str(tmp_path), "retail", LOAD_ID, ["EPMR"])

    assert counts == {"files": 2, "rows": 7}
    assert Path(landing_file(str(tmp_path), "retail", LOAD_ID, 0)).exists()
    assert Path(landing_file(str(tmp_path), "retail", LOAD_ID, 5)).exists()


def test_fetch_dataset_asks_the_url_of_its_dataset(tmp_path):
    client = FakeClient({0: page(rows(1), total=1)})

    fetch_dataset(client, str(tmp_path), "spot", LOAD_ID, ["EPCWTI"])

    assert [url for url, _ in client.requests] == [data_url("spot")]


def test_fetch_dataset_with_no_data_saves_nothing(tmp_path):
    client = FakeClient({0: page([], total=0)})

    counts = fetch_dataset(client, str(tmp_path), "supply", LOAD_ID, ["EPM0F"])

    assert counts == {"files": 0, "rows": 0}
    assert list(tmp_path.iterdir()) == []


STARTED_AT = datetime(2026, 11, 12, 9, 0, 0, tzinfo=UTC)


def test_make_load_id_has_compact_utc_format():
    assert make_load_id(STARTED_AT) == "20261112T090000Z"


def test_make_load_id_converts_local_time_to_utc():
    kyiv_winter = timezone(timedelta(hours=2))
    started_in_kyiv = datetime(2026, 11, 12, 11, 0, 0, tzinfo=kyiv_winter)

    assert make_load_id(started_in_kyiv) == "20261112T090000Z"


def test_write_marker_saves_receipt_with_counts(tmp_path):
    counts = {
        "supply": {"files": 1, "rows": 1450},
        "retail": {"files": 1, "rows": 320},
        "spot": {"files": 0, "rows": 0},
    }

    path = write_marker(str(tmp_path), LOAD_ID, STARTED_AT, counts, start="2026-09-01")

    assert path == marker_file(str(tmp_path), LOAD_ID)
    assert json.loads(Path(path).read_text(encoding="utf-8")) == {
        "load_id": LOAD_ID,
        "started_at": "2026-11-12T09:00:00+00:00",
        "start": "2026-09-01",
        "datasets": counts,
    }


def test_write_marker_without_start_means_full_history(tmp_path):
    path = write_marker(str(tmp_path), LOAD_ID, STARTED_AT, {})

    assert json.loads(Path(path).read_text(encoding="utf-8"))["start"] is None


REFERENCE_DIR = Path(__file__).parents[2] / "data" / "reference"


def write_csv(folder, name, text):
    (folder / f"{name}.csv").write_text(text, encoding="utf-8")


def test_products_for_supply_takes_every_product(tmp_path):
    write_csv(tmp_path, "ref_product", "product_bk,product_name\nEPM0F,Gasoline\nEPD0,Diesel\n")

    assert products_for("supply", tmp_path) == ["EPM0F", "EPD0"]


def test_products_for_prices_takes_only_their_price_type(tmp_path):
    write_csv(
        tmp_path,
        "ref_eia_product",
        "eia_product,price_type,grade_bk\n"
        "EPMR,retail,REGULAR\n"
        "EPMRU,spot,REGULAR\n"
        "EPD2DXL0,retail,ULSD\n"
        "EPD2DXL0,spot,ULSD\n",
    )

    assert products_for("retail", tmp_path) == ["EPMR", "EPD2DXL0"]
    assert products_for("spot", tmp_path) == ["EPMRU", "EPD2DXL0"]


@pytest.mark.parametrize("dataset", list(DATASETS))
def test_every_dataset_has_products_in_repo_reference_files(dataset):
    assert products_for(dataset, REFERENCE_DIR)


TODAY = date(2026, 11, 12)


def test_start_for_first_load_takes_full_history(tmp_path):
    assert start_for(str(tmp_path), TODAY) is None


def test_start_for_after_finished_load_takes_last_weeks(tmp_path):
    write_marker(str(tmp_path), LOAD_ID, STARTED_AT, {})

    assert start_for(str(tmp_path), TODAY) == "2026-09-17"


def test_start_for_uses_given_number_of_weeks(tmp_path):
    write_marker(str(tmp_path), LOAD_ID, STARTED_AT, {})

    assert start_for(str(tmp_path), TODAY, lookback_weeks=2) == "2026-10-29"


def test_start_for_ignores_load_that_failed_before_its_marker(tmp_path):
    write_page(str(tmp_path), "supply", LOAD_ID, 0, [{"value": "1"}])

    assert start_for(str(tmp_path), TODAY) is None


def prepare_reference(folder):
    write_csv(folder, "ref_product", "product_bk\nEPM0F\n")
    write_csv(folder, "ref_eia_product", "eia_product,price_type\nEPMR,retail\nEPMRU,spot\n")


def test_run_load_fetches_every_dataset_then_writes_marker(tmp_path):
    prepare_reference(tmp_path)
    landing = str(tmp_path / "landing")
    client = FakeClient({0: page(rows(2), total=2)})

    load_id, counts = run_load(client, landing, tmp_path, STARTED_AT)

    assert load_id == "20261112T090000Z"
    assert counts == {
        "supply": {"files": 1, "rows": 2},
        "retail": {"files": 1, "rows": 2},
        "spot": {"files": 1, "rows": 2},
    }
    marker = json.loads(Path(marker_file(landing, load_id)).read_text(encoding="utf-8"))
    assert marker["datasets"] == counts
    assert marker["start"] is None


def test_run_load_asks_each_dataset_for_its_own_products(tmp_path):
    prepare_reference(tmp_path)
    client = FakeClient({0: page(rows(1), total=1)})

    run_load(client, str(tmp_path / "landing"), tmp_path, STARTED_AT)

    asked = {url: [value for key, value in params if key == "facets[product][]"] for url, params in client.requests}
    assert asked == {
        data_url("supply"): ["EPM0F"],
        data_url("retail"): ["EPMR"],
        data_url("spot"): ["EPMRU"],
    }


def test_run_load_after_finished_load_takes_last_weeks(tmp_path):
    prepare_reference(tmp_path)
    landing = str(tmp_path / "landing")
    client = FakeClient({0: page(rows(1), total=1)})
    run_load(client, landing, tmp_path, STARTED_AT)

    next_week = datetime(2026, 11, 19, 9, 0, 0, tzinfo=UTC)
    load_id, _ = run_load(client, landing, tmp_path, next_week)

    marker = json.loads(Path(marker_file(landing, load_id)).read_text(encoding="utf-8"))
    assert marker["start"] == "2026-09-24"


def test_run_load_writes_no_marker_when_a_dataset_fails(tmp_path):
    prepare_reference(tmp_path)
    landing = str(tmp_path / "landing")
    client = FakeClient({0: page([], total=5)})

    with pytest.raises(ValueError):
        run_load(client, landing, tmp_path, STARTED_AT)

    assert not Path(marker_file(landing, "20261112T090000Z")).exists()
