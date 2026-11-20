"""EIA pages for the RAG assistant, landed in the volume as JSON for the bronze pipeline (Auto Loader).

Landing is append-only: an article is landed once, a STEO page once per new edition.
One file per page: <root>/eia_articles/load_id=<load_id>/<doc_type>_<doc_key>.json
"""

import hashlib
import json
from pathlib import Path

ARTICLE = "article"
OUTLOOK = "outlook"
LANDING_FOLDER = "eia_articles"
LANDING_SCHEMA = (
    "source_url STRING, doc_type STRING, doc_key STRING, listed_title STRING, listed_date STRING, "
    "source_tags ARRAY<STRING>, raw_html STRING, content_hash STRING, fetched_at STRING, load_id STRING"
)


def content_hash(html):
    return hashlib.sha256(html.encode("utf-8")).hexdigest()


def article_record(listed, html, fetched_at, load_id):
    """Landed record of a Today in Energy article found on tag pages (a discovery.ListedArticle)."""
    return {
        "source_url": listed.url,
        "doc_type": ARTICLE,
        "doc_key": str(listed.article_id),
        "listed_title": listed.title,
        "listed_date": listed.listed_date.isoformat(),
        "source_tags": list(listed.tags),
        "raw_html": html,
        "content_hash": content_hash(html),
        "fetched_at": fetched_at.isoformat(),
        "load_id": load_id,
    }


def steo_record(page_name, url, html, fetched_at, load_id):
    """Landed record of a STEO page; silver reads the release date from the HTML."""
    return {
        "source_url": url,
        "doc_type": OUTLOOK,
        "doc_key": page_name,
        "listed_title": None,
        "listed_date": None,
        "source_tags": [],
        "raw_html": html,
        "content_hash": content_hash(html),
        "fetched_at": fetched_at.isoformat(),
        "load_id": load_id,
    }


def landing_file(root, record):
    return f"{root}/{LANDING_FOLDER}/load_id={record['load_id']}/{record['doc_type']}_{record['doc_key']}.json"


def write_landing(root, record):
    """Save one page as a single JSON line."""
    path = Path(landing_file(root, record))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record) + "\n", encoding="utf-8")
    return str(path)


def landed_files(root, doc_type):
    """Landed files of one doc type, oldest load first (load ids are UTC timestamps, so they sort as text)."""
    return sorted(Path(root, LANDING_FOLDER).glob(f"load_id=*/{doc_type}_*.json"), key=lambda path: path.parent.name)


def landed_article_ids(root):
    """Ids of the Today in Energy articles that are already landed."""
    return {int(path.stem.removeprefix(f"{ARTICLE}_")) for path in landed_files(root, ARTICLE)}


def latest_steo_hashes(root):
    """content_hash of the newest landed version of each STEO page."""
    hashes = {}
    for path in landed_files(root, OUTLOOK):
        record = json.loads(path.read_text(encoding="utf-8"))
        hashes[record["doc_key"]] = record["content_hash"]
    return hashes


def articles_to_fetch(listed_articles, landed_ids):
    """Articles that are not landed yet. Landed articles are never fetched again."""
    return [article for article in listed_articles if article.article_id not in landed_ids]


def is_new_edition(record, latest_hashes):
    """True when a STEO page differs from the newest landed version of the same page (or was never landed)."""
    return latest_hashes.get(record["doc_key"]) != record["content_hash"]


def too_many_failures(attempted, failed, max_ratio=0.1):
    """A run fails when more than max_ratio of its page fetches failed; a few failures are retried next run."""
    return attempted > 0 and failed / attempted > max_ratio
