from datetime import UTC, date, datetime
from pathlib import Path

from rag_testing import terraform_columns

from refinery.rag.silver import PARSE_ERROR, page_records, split_pages

FIXTURES = Path(__file__).parent / "fixtures"
PROCESSED_AT = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


def bronze_row(doc_type="article", doc_key="61142", html_file="tie_article_61142.html", raw_html=None):
    return {
        "doc_type": doc_type,
        "doc_key": doc_key,
        "source_url": f"https://www.eia.gov/page/{doc_key}",
        "listed_title": "Listed title",
        "raw_html": raw_html if raw_html is not None else (FIXTURES / html_file).read_text(encoding="utf-8"),
        "content_hash": "abc",
        "load_id": "20261120T120000Z",
    }


def test_article_goes_to_silver_normalized():
    silver, quarantined = page_records(bronze_row(), PROCESSED_AT)

    assert quarantined is None
    assert silver["doc_id"] == "article_61142"
    assert silver["published_date"] == date(2024, 1, 2)
    assert silver["contributors"] == "Matthew French"
    assert silver["tags"][:2] == ["Brent", "crude oil"]
    assert "the country's voluntary production cut" in silver["body_text"]
    assert silver["word_count"] == len(silver["body_text"].split())
    assert silver["processed_at"] == PROCESSED_AT


def test_steo_page_goes_to_silver_with_its_next_release_date():
    silver, _ = page_records(bronze_row("outlook", "global_oil", "steo_global_oil.html"), PROCESSED_AT)

    assert silver["doc_id"] == "outlook_global_oil"
    assert silver["published_date"] == date(2026, 11, 10)
    assert silver["next_release_date"] == date(2026, 12, 8)
    assert silver["tags"] == []


def test_off_topic_article_goes_to_quarantine_with_the_reason():
    html = (
        '<div class="tie-article"><span class="date">May 1, 2026</span><h1>Natural gas storage rose</h1>'
        + "<p>Natural gas storage rose as LNG exports and power demand grew this winter.</p>" * 10
        + "<p><strong>Principal contributor: </strong>EIA Staff</p></div>"
    )

    silver, quarantined = page_records(bronze_row(raw_html=html), PROCESSED_AT)

    assert silver is None
    assert quarantined["reject_reason"] == "off_topic"
    assert quarantined["title"] == "Natural gas storage rose"
    assert quarantined["word_count"] == 130
    assert quarantined["topic_mentions"] == 0


def test_page_that_cannot_be_parsed_goes_to_quarantine_with_the_error():
    silver, quarantined = page_records(bronze_row(raw_html="<html><body>Not found</body></html>"), PROCESSED_AT)

    assert silver is None
    assert quarantined["reject_reason"] == PARSE_ERROR
    assert "No article" in quarantined["reject_detail"]
    assert quarantined["title"] == "Listed title"


def test_split_pages_puts_each_page_in_one_table():
    rows = [bronze_row(), bronze_row(doc_key="1", raw_html="<p>broken</p>")]

    accepted, rejected = split_pages(rows, PROCESSED_AT)

    assert [record["doc_id"] for record in accepted] == ["article_61142"]
    assert [record["doc_id"] for record in rejected] == ["article_1"]


def test_records_have_exactly_the_columns_of_the_terraform_tables():
    silver, _ = page_records(bronze_row(), PROCESSED_AT)
    _, quarantined = page_records(bronze_row(raw_html="<p>broken</p>"), PROCESSED_AT)

    assert set(silver) == set(terraform_columns("eia_articles"))
    assert set(quarantined) == set(terraform_columns("eia_articles_quarantine"))