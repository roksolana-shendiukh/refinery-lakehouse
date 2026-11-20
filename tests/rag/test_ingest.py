import json
from datetime import UTC, date, datetime

import pytest
import requests

from refinery.rag.discovery import tag_page_url
from refinery.rag.eia_site import PageNotFound
from refinery.rag.ingest import run_fetch

STARTED_AT = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
LATER = datetime(2026, 11, 27, 12, 0, tzinfo=UTC)
SINCE = date(2024, 1, 1)
STEO = {"global_oil": "https://www.eia.gov/outlooks/steo/report/global_oil.php"}


def tag_page(*entries):
    """A tag page with (article id, 'Month D, YYYY') entries."""
    items = "".join(
        f'<span class="date">{listed_on}</span><h2><a href="detail.php?id={article_id}">Article {article_id}</a></h2>'
        for article_id, listed_on in entries
    )
    return f'<div class="tie-article tag-view">{items}</div>'


def article_url(article_id):
    return f"https://www.eia.gov/todayinenergy/detail.php?id={article_id}"


class FakeClient:
    """Returns the page planned for a URL, or raises it when it is an exception; records every URL."""

    def __init__(self, pages):
        self.pages = pages
        self.requested = []

    def get_text(self, url):
        self.requested.append(url)
        page = self.pages[url]
        if isinstance(page, Exception):
            raise page
        return page


def site(articles=(), steo="<p>November</p>", **extra):
    """Two tags that both list article 1, plus article pages and the STEO page."""
    pages = {
        tag_page_url("crude oil"): tag_page((2, "November 12, 2026"), (1, "March 3, 2025"), (9, "December 28, 2023")),
        tag_page_url("gasoline"): tag_page((1, "March 3, 2025")),
        STEO["global_oil"]: steo,
    }
    pages.update({article_url(article_id): f"<p>article {article_id}</p>" for article_id in articles})
    pages.update(extra)
    return pages


def fetch(client, root, started_at=STARTED_AT):
    return run_fetch(client, root, started_at, tags=("crude oil", "gasoline"), since=SINCE, steo_pages=STEO)


def landed(root):
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*.json"))


def test_first_run_lands_every_listed_article_since_the_start_date_and_the_steo_page(tmp_path):
    load_id, counts = fetch(FakeClient(site(articles=(1, 2))), tmp_path)

    assert load_id == "20261120T120000Z"
    assert counts == {"listed": 2, "already_landed": 0, "landed": 2, "not_found": 0, "failed": 0, "steo_new": 1}
    assert landed(tmp_path) == [
        "eia_articles/load_id=20261120T120000Z/article_1.json",
        "eia_articles/load_id=20261120T120000Z/article_2.json",
        "eia_articles/load_id=20261120T120000Z/outlook_global_oil.json",
    ]


def test_an_article_listed_under_two_tags_is_landed_once_with_both_tags(tmp_path):
    fetch(FakeClient(site(articles=(1, 2))), tmp_path)

    path = tmp_path / "eia_articles/load_id=20261120T120000Z/article_1.json"
    assert json.loads(path.read_text(encoding="utf-8"))["source_tags"] == ["crude oil", "gasoline"]


def test_second_run_fetches_nothing_new_when_nothing_changed(tmp_path):
    fetch(FakeClient(site(articles=(1, 2))), tmp_path)
    client = FakeClient(site(articles=(1, 2)))

    _, counts = fetch(client, tmp_path, LATER)

    assert counts == {"listed": 2, "already_landed": 2, "landed": 0, "not_found": 0, "failed": 0, "steo_new": 0}
    assert article_url(1) not in client.requested
    assert all(path.startswith("eia_articles/load_id=20261120T120000Z/") for path in landed(tmp_path))


def test_a_new_steo_edition_is_landed_in_the_new_load(tmp_path):
    fetch(FakeClient(site(articles=(1, 2))), tmp_path)

    _, counts = fetch(FakeClient(site(articles=(1, 2), steo="<p>December</p>")), tmp_path, LATER)

    assert counts["steo_new"] == 1
    assert "eia_articles/load_id=20261127T120000Z/outlook_global_oil.json" in landed(tmp_path)


def test_a_missing_article_is_skipped_and_counted(tmp_path):
    pages = site(articles=(1,), **{article_url(2): PageNotFound("redirects to /todayinenergy/")})

    _, counts = fetch(FakeClient(pages), tmp_path)

    assert counts["landed"] == 1
    assert counts["not_found"] == 1
    assert counts["failed"] == 0


def test_the_run_fails_when_too_many_fetches_fail(tmp_path):
    pages = site(articles=(1,), **{article_url(2): requests.HTTPError("HTTP 503")})

    with pytest.raises(RuntimeError, match="1 of 3 page fetches failed"):
        fetch(FakeClient(pages), tmp_path)


def test_a_tag_page_that_fails_stops_the_run(tmp_path):
    pages = site(articles=(1, 2), **{tag_page_url("gasoline"): requests.HTTPError("HTTP 503")})

    with pytest.raises(requests.HTTPError):
        fetch(FakeClient(pages), tmp_path)
    assert landed(tmp_path) == []
