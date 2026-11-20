import json
from datetime import UTC, date, datetime
from pathlib import Path

from refinery.rag.bronze import (
    LANDING_SCHEMA,
    article_record,
    articles_to_fetch,
    content_hash,
    is_new_edition,
    landed_article_ids,
    landing_file,
    latest_steo_hashes,
    steo_record,
    too_many_failures,
    write_landing,
)
from refinery.rag.discovery import ListedArticle

FETCHED_AT = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
STEO_URL = "https://www.eia.gov/outlooks/steo/report/global_oil.php"
RECORD_FIELDS = [
    "source_url",
    "doc_type",
    "doc_key",
    "listed_title",
    "listed_date",
    "source_tags",
    "raw_html",
    "content_hash",
    "fetched_at",
    "load_id",
]


def listed(article_id, tags=("crude oil",)):
    return ListedArticle(
        article_id=article_id,
        url=f"https://www.eia.gov/todayinenergy/detail.php?id={article_id}",
        title=f"Article {article_id}",
        listed_date=date(2026, 11, 12),
        tags=tags,
    )


def steo(html, load_id, page_name="global_oil"):
    return steo_record(page_name, STEO_URL, html, FETCHED_AT, load_id)


def test_landing_schema_has_a_column_for_every_record_field():
    columns = [column.split()[0] for column in LANDING_SCHEMA.split(", ")]

    assert columns == RECORD_FIELDS


def test_content_hash_depends_only_on_the_html():
    assert content_hash("<p>a</p>") == content_hash("<p>a</p>")
    assert content_hash("<p>a</p>") != content_hash("<p>b</p>")


def test_article_record_keeps_what_the_tag_pages_listed():
    record = article_record(listed(68444, ("crude oil", "oil/petroleum")), "<html/>", FETCHED_AT, "20261120T120000Z")

    assert list(record) == RECORD_FIELDS
    assert record["source_url"] == "https://www.eia.gov/todayinenergy/detail.php?id=68444"
    assert record["doc_type"] == "article"
    assert record["doc_key"] == "68444"
    assert record["listed_title"] == "Article 68444"
    assert record["listed_date"] == "2026-11-12"
    assert record["source_tags"] == ["crude oil", "oil/petroleum"]
    assert record["content_hash"] == content_hash("<html/>")
    assert record["fetched_at"] == "2026-11-20T12:00:00+00:00"


def test_steo_record_has_the_same_fields_and_no_listing_data():
    record = steo("<html/>", "20261120T120000Z")

    assert list(record) == RECORD_FIELDS
    assert record["doc_type"] == "outlook"
    assert record["doc_key"] == "global_oil"
    assert record["listed_title"] is None
    assert record["listed_date"] is None
    assert record["source_tags"] == []


def test_landing_file_is_one_file_per_page_in_the_load_folder():
    record = article_record(listed(68444), "<html/>", FETCHED_AT, "20261120T120000Z")

    path = landing_file("/Volumes/c/s/landing", record)

    assert path == "/Volumes/c/s/landing/eia_articles/load_id=20261120T120000Z/article_68444.json"


def test_write_landing_saves_the_record_as_one_json_line(tmp_path):
    record = article_record(listed(68444), "<p>text</p>", FETCHED_AT, "20261120T120000Z")

    path = write_landing(tmp_path, record)

    lines = Path(path).read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == [record]


def test_landed_article_ids_reads_every_load(tmp_path):
    write_landing(tmp_path, article_record(listed(1), "<html/>", FETCHED_AT, "20261120T120000Z"))
    write_landing(tmp_path, article_record(listed(2), "<html/>", FETCHED_AT, "20261127T120000Z"))
    write_landing(tmp_path, steo("<html/>", "20261127T120000Z"))

    assert landed_article_ids(tmp_path) == {1, 2}


def test_landed_article_ids_is_empty_before_the_first_load(tmp_path):
    assert landed_article_ids(tmp_path) == set()


def test_latest_steo_hashes_takes_the_newest_load_of_each_page(tmp_path):
    write_landing(tmp_path, steo("<p>October</p>", "20261013T120000Z"))
    write_landing(tmp_path, steo("<p>November</p>", "20261110T120000Z"))
    write_landing(tmp_path, steo("<p>products</p>", "20261013T120000Z", page_name="petro_prod"))

    assert latest_steo_hashes(tmp_path) == {
        "global_oil": content_hash("<p>November</p>"),
        "petro_prod": content_hash("<p>products</p>"),
    }


def test_articles_to_fetch_skips_landed_articles():
    assert [a.article_id for a in articles_to_fetch([listed(1), listed(2), listed(3)], {2})] == [1, 3]


def test_is_new_edition_for_a_page_never_landed():
    assert is_new_edition(steo("<html/>", "20261120T120000Z"), {})


def test_is_new_edition_compares_with_the_newest_landed_version_of_the_same_page():
    record = steo("<p>November</p>", "20261120T120000Z")

    assert not is_new_edition(record, {"global_oil": content_hash("<p>November</p>")})
    assert is_new_edition(record, {"global_oil": content_hash("<p>October</p>")})
    assert is_new_edition(record, {"petro_prod": content_hash("<p>November</p>")})


def test_too_many_failures_allows_up_to_ten_percent():
    assert not too_many_failures(attempted=100, failed=10)
    assert too_many_failures(attempted=100, failed=11)


def test_too_many_failures_is_false_when_nothing_was_attempted():
    assert not too_many_failures(attempted=0, failed=0)
