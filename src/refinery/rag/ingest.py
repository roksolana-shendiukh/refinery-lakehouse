"""One run of the RAG ingest: find Today in Energy articles, land the new ones and any new STEO edition."""

import logging
from datetime import UTC, datetime

import requests

from refinery.bronze.eia import make_load_id
from refinery.rag.bronze import (
    article_record,
    articles_to_fetch,
    is_new_edition,
    landed_article_ids,
    latest_steo_hashes,
    steo_record,
    too_many_failures,
    write_landing,
)
from refinery.rag.discovery import merge_tags, parse_tag_page, published_since, tag_page_url
from refinery.rag.eia_site import PageNotFound
from refinery.rag.sources import ARTICLES_SINCE, STEO_PAGES, TIE_TAGS

logger = logging.getLogger(__name__)


def utc_now():
    return datetime.now(UTC)


def disvocer_articles(client, tags, since):
    """Articles listed on the tag pages since `since`, one entry per article. A tag page that fails stops the run."""
    articles_by_tag = {}
    for tag in tags:
        listed = published_since(
            parse_tag_page(
                client.get_text(tag_page_url(tag))
            ), since
        )
        logger.info("Tag %r lists %d articles since %s", tag, len(listed), since)
        articles_by_tag[tag] = listed
    return merge_tags(articles_by_tag)


def run_fetch(
        client, landing_root, started_at, tags=TIE_TAGS, since=ARTICLES_SINCE, steo_pages=STEO_PAGES, now=utc_now
):
    """Land new articles and new STEO editions; return the counts. Fails when too many page fetches failed."""
    load_id = make_load_id(started_at)
    counts = {"listed": 0, "already_landed": 0, "landed": 0, "not_found": 0, "failed": 0, "steo_new": 0}

    listed = disvocer_articles(client, tags, since)
    to_fetch = articles_to_fetch(listed, landed_article_ids(landing_root))
    counts["listed"] = len(listed)
    counts["already_landed"] = len(listed) - len(to_fetch)

    for article in to_fetch:
        try:
            html = client.get_text(article.url)
        except PageNotFound as error:
            logger.warning("Skipping article %d: %s", article.article_id, error)
            counts["not_found"] += 1
            continue
        except requests.RequestException as error:
            logger.error("Could not fetch article %d: %s", article.article_id, error)
            counts["failed"] += 1
            continue
        write_landing(landing_root, article_record(article, html, now(), load_id))
        counts["landed"] += 1

    latest = latest_steo_hashes(landing_root)
    for page_name, url in steo_pages.items():
        try:
            record = steo_record(page_name, url, client.get_text(url), now(), load_id)
        except requests.RequestException as error:
            logger.error("Could not fetch STEO page %s: %s", page_name, error)
            counts["failed"] += 1
            continue
        if is_new_edition(record, latest):
            write_landing(landing_root, record)
            counts["steo_new"] += 1

    attempted = len(to_fetch) + len(steo_pages)
    if too_many_failures(attempted, counts["failed"]):
        raise RuntimeError(f"Load {load_id}: {counts['failed']} of {attempted} page fetches failed: {counts}")
    return load_id, counts
