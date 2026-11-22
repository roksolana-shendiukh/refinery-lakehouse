"""Splits silver EIA pages into overlapping chunks for the vector search index.

A chunk is whole paragraphs (a [Chart: ...] line with its source note is a paragraph too) up to MAX_WORDS.
A paragraph longer than that is split by sentences. The next chunk starts with the end of the previous one
(whole sentences, up to OVERLAP_WORDS), so a fact on a chunk border is found from both sides.
Every chunk starts with "Title (Month D, YYYY)", so the chunk alone says what it is from and when.

The chunk_articles task writes the chunks of all silver pages into rag eia_article_chunks (MERGE by chunk_id).
"""

import re
from datetime import UTC, datetime

from refinery.rag.tables import merge_records, require_tables

MAX_WORDS = 300
OVERLAP_WORDS = 50
SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"(\[$])")


def word_count(text):
    return len(text.split())


def split_long(paragraph, max_words):
    """A paragraph of more than max_words as pieces of whole sentences (a too long sentence is cut by words)."""
    pieces, current = [], []
    for sentence in SENTENCE_END.split(paragraph):
        words = sentence.split()
        while len(words) > max_words:
            pieces.append(" ".join(words[:max_words]))
            words = words[max_words:]
        if current and word_count(" ".join(current)) + len(words) > max_words:
            pieces.append(" ".join(current))
            current = []
        if words:
            current.append(" ".join(words))
    if current:
        pieces.append(" ".join(current))
    return pieces


def text_units(body, max_words):
    """Paragraphs of the body, with each paragraph longer than max_words split into smaller pieces."""
    units = []
    for paragraph in body.split("\n\n"):
        paragraph = paragraph.strip()
        if not paragraph:
            continue
        units.extend([paragraph] if word_count(paragraph) <= max_words else split_long(paragraph, max_words))
    return units


def overlap_tail(units, overlap_words):
    """The end of the units, up to overlap_words: whole last paragraphs, then the last sentences of the one before."""
    tail = []
    words = 0
    for unit in reversed(units):
        if words + word_count(unit) <= overlap_words:
            tail.insert(0, unit)
            words += word_count(unit)
            continue
        sentences = []
        for sentence in reversed(SENTENCE_END.split(unit)):
            if words + word_count(sentence) > overlap_words:
                break
            sentences.insert(0, sentence)
            words += word_count(sentence)
        if sentences:
            tail.insert(0, " ".join(sentences))
        break
    return tail


def split_text(body, max_words=MAX_WORDS, overlap_words=OVERLAP_WORDS):
    """Chunk texts of a body: whole paragraphs up to max_words, each chunk after the first overlapping the last."""
    chunks, current = [], []
    for unit in text_units(body, max_words):
        if current and word_count("\n\n".join([*current, unit])) > max_words:
            chunks.append(current)
            current = overlap_tail(current, overlap_words)
            # The overlap gives way when it would leave no room for the next unit.
            if word_count("\n\n".join([*current, unit])) > max_words:
                current = []
        current.append(unit)
    if current:
        chunks.append(current)
    return ["\n\n".join(chunk) for chunk in chunks]


def chunk_header(title, published_date):
    return f"{title} ({published_date:%B} {published_date.day}, {published_date.year})"


def chunk_id(doc_id, index):
    return f"{doc_id}#{index:04d}"


def page_chunks(page, max_words=MAX_WORDS, overlap_words=OVERLAP_WORDS):
    """Chunk records of one silver page (a row of silver eia_articles, or a dict with its columns)."""
    header = chunk_header(page["title"], page["published_date"])
    return [
        {
            "chunk_id": chunk_id(page["doc_id"], index),
            "doc_id": page["doc_id"],
            "doc_type": page["doc_type"],
            "title": page["title"],
            "published_date": page["published_date"],
            "source_url": page["source_url"],
            "chunk_index": index,
            "chunk_text": f"{header}\n\n{text}",
        }
        for index, text in enumerate(split_text(page["body_text"], max_words, overlap_words))
    ]


def chunk_records(pages, processed_at):
    """Chunk records of the given silver pages, as they go into rag eia_article_chunks."""
    return [{**chunk, "processed_at": processed_at} for page in pages for chunk in page_chunks(page)]


def run_chunk(spark, silver_table, chunks_table, now=lambda: datetime.now(UTC)):
    """One run of chunk_articles; returns the counts of pages and chunks."""
    require_tables(spark, silver_table, chunks_table)
    pages = [row.asDict() for row in spark.table(silver_table).collect()]
    # An empty silver would delete every chunk, and the vector search index would follow.
    if not pages:
        raise RuntimeError(f"{silver_table} has no pages; nothing to chunk")

    records = chunk_records(pages, now())
    merge_records(spark, chunks_table, records, key="chunk_id")
    return {"pages": len(pages), "chunks": len(records)}
