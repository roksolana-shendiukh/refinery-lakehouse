"""Finds Today in Energy articles on tag pages: one page per tag lists every article with its date."""

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from urllib.parse import quote, urljoin

from bs4 import BeautifulSoup

from refinery.rag.sources import TIE_BASE_URL

ARTICLE_ID = re.compile(r"detail\.php\?id=(\d+)")
DATE_FORMAT = "%B %d, %Y"  # example: "November 6, 2026"


@dataclass(frozen=True)
class ListedArticle:
    article_id: int
    url: str
    title: str
    listed_date: date
    tags: tuple = ()


def tag_page_url(tag):
    """URL of the page that lists every article with this tag."""
    return f"{TIE_BASE_URL}index.php?tg={quote(tag)}"


def parse_tag_page(html):
    """Articles listed on a tag page, in page order (newest first).

    Each entry is a <span class="date"> followed by <h2><a href="detail.php?id=N">title</a></h2>,
    all inside div.tie-article.tag-view.
    """
    soup = BeautifulSoup(html, "html.parser")
    article_list = soup.select_one("div.tie-article.tag-view")
    if article_list is None:
        raise ValueError("The tag page has no article list (div.tie-article.tag-view)")

    articles = []
    for date_span in article_list.select("span.date"):
        date_text = date_span.get_text(strip=True)
        heading = date_span.find_next_sibling("h2")
        link = heading.find("a", href=True) if heading else None
        match = ARTICLE_ID.search(link["href"]) if link else None

        if match is None:
            raise ValueError(f"No article link after the date {date_text!r}")
        articles.append(
            ListedArticle(
                article_id=int(match.group(1)),
                url=urljoin(TIE_BASE_URL, link["href"]),
                title=link.get_text(" ", strip=True),
                listed_date=datetime.strptime(date_text, DATE_FORMAT).replace(tzinfo=UTC).date(),
            )
        )
    return articles


def published_since(articles, since):
    """Articles listed on or after `since`. Filters by date."""
    return [article for article in articles if article.listed_date >= since]


def merge_tags(articles_by_tag):
    """One entry per article id with every tag it was listed under, newest first.

    If tags list the same article with different dates, the earliest date wins.
    """
    merged = {}
    for tag, articles in articles_by_tag.items():
        for article in articles:
            known = merged.get(article.article_id)
            if known is None:
                merged[article.article_id] = ListedArticle(
                    article.article_id, article.url, article.title, article.listed_date, (tag,)
                )
            else:
                merged[article.article_id] = ListedArticle(
                    known.article_id,
                    known.url,
                    known.title,
                    min(known.listed_date, article.listed_date),
                    tuple(sorted({*known.tags, tag})),
                )
    return sorted(merged.values(), key=lambda art: (art.listed_date, art.article_id), reverse=True)
