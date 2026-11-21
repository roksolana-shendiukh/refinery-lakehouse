from datetime import date
from pathlib import Path

import pytest

from refinery.rag.discovery import ListedArticle, merge_tags, parse_tag_page, published_since, tag_page_url

TAG_PAGE = (Path(__file__).parent / "fixtures" / "tie_tag_page.html").read_text(encoding="utf-8")


def article(article_id, listed_date, tags=()):
    return ListedArticle(
        article_id=article_id,
        url=f"https://www.eia.gov/todayinenergy/detail.php?id={article_id}",
        title=f"Article {article_id}",
        listed_date=listed_date,
        tags=tags,
    )


def test_tag_page_url_encodes_spaces_and_keeps_slashes():
    assert tag_page_url("crude oil") == "https://www.eia.gov/todayinenergy/index.php?tg=crude%20oil"
    assert tag_page_url("oil/petroleum") == "https://www.eia.gov/todayinenergy/index.php?tg=oil/petroleum"


def test_parse_tag_page_reads_every_listed_article_in_page_order():
    articles = parse_tag_page(TAG_PAGE)

    assert [a.article_id for a in articles] == [68444, 68425, 61143, 61108, 210]


def test_parse_tag_page_reads_date_title_and_absolute_url():
    first = parse_tag_page(TAG_PAGE)[0]

    assert first.listed_date == date(2026, 11, 12)
    assert first.title == "EIA forecasts Alaskan crude oil production to grow 13% in 2027"
    assert first.url == "https://www.eia.gov/todayinenergy/detail.php?id=68444"


def test_parse_tag_page_reads_single_digit_days():
    assert parse_tag_page(TAG_PAGE)[1].listed_date == date(2026, 11, 6)


def test_parse_tag_page_ignores_article_links_outside_the_list():
    assert 99999 not in [a.article_id for a in parse_tag_page(TAG_PAGE)]


def test_parse_tag_page_fails_without_the_article_list():
    with pytest.raises(ValueError, match="no article list"):
        parse_tag_page("<html><body><p>Maintenance</p></body></html>")


def test_parse_tag_page_fails_when_a_date_has_no_article_link():
    html = '<div class="tie-article tag-view"><span class="date">May 1, 2026</span><h2>No link</h2></div>'

    with pytest.raises(ValueError, match="May 1, 2026"):
        parse_tag_page(html)


def test_published_since_keeps_articles_from_the_start_date_on():
    articles = parse_tag_page(TAG_PAGE)

    recent = published_since(articles, date(2024, 1, 1))

    assert [a.article_id for a in recent] == [68444, 68425, 61143]


def test_published_since_uses_dates_not_ids():
    older_with_higher_id = article(62444, date(2024, 7, 3))
    newer_with_lower_id = article(62443, date(2024, 7, 8))

    recent = published_since([older_with_higher_id, newer_with_lower_id], date(2024, 7, 5))

    assert recent == [newer_with_lower_id]


def test_merge_tags_keeps_one_entry_per_article_with_all_its_tags():
    merged = merge_tags(
        {
            "oil/petroleum": [article(1, date(2026, 1, 5)), article(2, date(2026, 1, 1))],
            "crude oil": [article(1, date(2026, 1, 5))],
        }
    )

    assert [(a.article_id, a.tags) for a in merged] == [(1, ("crude oil", "oil/petroleum")), (2, ("oil/petroleum",))]


def test_merge_tags_sorts_newest_first():
    merged = merge_tags({"gasoline": [article(5, date(2025, 3, 1)), article(9, date(2026, 2, 1))]})

    assert [a.article_id for a in merged] == [9, 5]


def test_merge_tags_keeps_the_earliest_date_when_tags_disagree():
    merged = merge_tags(
        {"oil/petroleum": [article(7, date(2025, 9, 3))], "liquid fuels": [article(7, date(2025, 12, 30))]}
    )

    assert merged[0].listed_date == date(2025, 9, 3)
