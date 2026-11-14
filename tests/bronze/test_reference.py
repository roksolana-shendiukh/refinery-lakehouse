import csv
import logging
from pathlib import Path

import pytest

from refinery.bronze.reference import (
    REFERENCE_COLUMNS,
    content_hash,
    land_reference,
    landed_reference_file,
)

REFERENCE_DIR = Path(__file__).parents[2] / "data" / "reference"


def write_csv(folder, name, text):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{name}.csv"
    path.write_text(text, encoding="utf-8")
    return path


def test_content_hash_is_the_same_for_the_same_content(tmp_path):
    first = write_csv(tmp_path, "first", "hub_bk\nY35NY\n")
    second = write_csv(tmp_path, "second", "hub_bk\nY35NY\n")

    assert content_hash(first) == content_hash(second)


def test_content_hash_changes_when_content_changes(tmp_path):
    before = content_hash(write_csv(tmp_path, "ref_hub", "hub_bk\nY35NY\n"))
    after = content_hash(write_csv(tmp_path, "ref_hub", "hub_bk\nY35NY\nRGC\n"))

    assert before != after


def test_landed_reference_file_path():
    path = landed_reference_file("/Volumes/raw", "ref_hub", "a3f5c1d2e4b6f708")

    assert path == "/Volumes/raw/reference/ref_hub/sha256=a3f5c1d2e4b6f708/ref_hub.csv"


def test_land_reference_copies_every_csv(tmp_path):
    repo = tmp_path / "repo"
    write_csv(repo, "ref_hub", "hub_bk\nY35NY\n")
    write_csv(repo, "ref_mid", "mid\n201\n")
    volume = str(tmp_path / "volume")

    landed = land_reference(repo, volume)

    assert landed == ["ref_hub", "ref_mid"]
    copied = Path(landed_reference_file(volume, "ref_hub", content_hash(repo / "ref_hub.csv")))
    assert copied.read_text(encoding="utf-8") == "hub_bk\nY35NY\n"


def test_land_reference_skips_files_that_did_not_change(tmp_path):
    repo = tmp_path / "repo"
    write_csv(repo, "ref_hub", "hub_bk\nY35NY\n")
    volume = str(tmp_path / "volume")
    land_reference(repo, volume)

    assert land_reference(repo, volume) == []


def test_land_reference_lands_a_new_version_when_csv_changes(tmp_path):
    repo = tmp_path / "repo"
    write_csv(repo, "ref_hub", "hub_bk\nY35NY\n")
    volume = str(tmp_path / "volume")
    land_reference(repo, volume)

    write_csv(repo, "ref_hub", "hub_bk\nY35NY\nRGC\n")

    assert land_reference(repo, volume) == ["ref_hub"]
    assert len(list((tmp_path / "volume" / "reference" / "ref_hub").iterdir())) == 2


def test_every_reference_csv_in_the_repo_has_a_contract():
    files = sorted(path.stem for path in REFERENCE_DIR.glob("*.csv"))

    assert files == sorted(REFERENCE_COLUMNS)


@pytest.mark.parametrize("name", sorted(REFERENCE_COLUMNS))
def test_reference_csv_header_matches_the_stm(name):
    with open(REFERENCE_DIR / f"{name}.csv", encoding="utf-8", newline="") as f:
        header = next(csv.reader(f))

    assert header == REFERENCE_COLUMNS[name]


def test_land_reference_logs_each_landed_file(tmp_path, caplog):
    repo = tmp_path / "repo"
    write_csv(repo, "ref_hub", "hub_bk\nY35NY\n")

    with caplog.at_level(logging.INFO):
        land_reference(repo, str(tmp_path / "volume"))

    assert f"Landed reference ref_hub (sha256={content_hash(repo / 'ref_hub.csv')})" in caplog.text
