"""Bronze EIA pages to silver eia_articles: parsed, normalized and checked; the pages left out go to quarantine.

Every run reads the latest landed version of every page, so a page is always either in silver or in quarantine.
MERGE by doc_id changes only rows whose content changed, which keeps the change feed for the chunks small.
"""

import logging
from datetime import UTC, datetime

from refinery.rag.bronze import ARTICLE, too_many_failures
from refinery.rag.clean import check_page, normalize_page
from refinery.rag.parse import PageParseError, parse_article, parse_steo

logger = logging.getLogger(__name__)

PARSE_ERROR = "parse_error"
UNCOMPARED_COLUMNS = {"processed_at"}  # a row that only got a new processed_at is not a change

LATEST_PAGES_SQL = """
    SELECT doc_type, doc_key, source_url, listed_title, raw_html, content_hash, load_id
    FROM IDENTIFIER(:bronze_table)
    QUALIFY row_number() OVER (PARTITION BY doc_type, doc_key ORDER BY load_id DESC) = 1
"""


def doc_id(doc_type, doc_key):
    return f"{doc_type}_{doc_key}"


def page_records(row, processed_at):
    """(silver record, None) for a page that goes into the knowledge base, (None, quarantine record) otherwise."""
    common = {
        "doc_id": doc_id(row["doc_type"], row["doc_key"]),
        "doc_type": row["doc_type"],
        "doc_key": row["doc_key"],
        "source_url": row["source_url"],
        "content_hash": row["content_hash"],
        "load_id": row["load_id"],
        "processed_at": processed_at,
    }
    parse = parse_article if row["doc_type"] == ARTICLE else parse_steo
    try:
        page = normalize_page(parse(row["raw_html"]))
    except PageParseError as error:
        return None, {
            **common,
            "title": row["listed_title"],
            "published_date": None,
            "reject_reason": PARSE_ERROR,
            "reject_detail": str(error),
            "word_count": None,
            "topic_mentions": None,
        }

    check = check_page(row["doc_type"], page)
    if not check.accepted:
        return None, {
            **common,
            "title": page.title,
            "published_date": page.published_date,
            "reject_reason": check.reject_reason,
            "reject_detail": None,
            "word_count": check.word_count,
            "topic_mentions": check.topic_mentions,
        }
    return {
        **common,
        "title": page.title,
        "published_date": page.published_date,
        "next_release_date": page.next_release_date,
        "tags": list(page.tags),
        "contributors": page.contributors,
        "body_text": page.body,
        "word_count": check.word_count,
    }, None


def split_pages(rows, processed_at):
    """Silver and quarantine records of the given bronze rows."""
    accepted, rejected = [], []
    for row in rows:
        silver, quarantined = page_records(row, processed_at)
        if silver:
            accepted.append(silver)
        else:
            rejected.append(quarantined)
    return accepted, rejected


def merge_sql(columns):
    """MERGE of the updates view into the target: changed rows updated, new inserted, pages gone deleted."""
    changed = " OR ".join(f"NOT (t.{column} <=> s.{column})" for column in columns if column not in UNCOMPARED_COLUMNS)
    return f"""
        MERGE INTO IDENTIFIER(:target_table) AS t
        USING rag_updates AS s
        ON t.doc_id = s.doc_id
        WHEN MATCHED AND ({changed}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE
    """


def merge_records(spark, table_name, records):
    schema = spark.table(table_name).schema
    rows = [tuple(record.get(field.name) for field in schema.fields) for record in records]
    spark.createDataFrame(rows, schema).createOrReplaceTempView("rag_updates")

    result = spark.sql(merge_sql(schema.fieldNames()), args={"target_table": table_name}).collect()
    logger.info("%s: %s", table_name, result[0].asDict() if result else {})


def run_clean(spark, bronze_table, silver_table, quarantine_table, now=lambda: datetime.now(UTC)):
    """One run of clean_articles; returns the counts of kept and rejected pages by reason."""
    for table_name in (silver_table, quarantine_table):
        if not spark.catalog.tableExists(table_name):
            raise RuntimeError(f"{table_name} does not exist; it is created by Terraform (infra/rag_tables.tf)")

    rows = spark.sql(LATEST_PAGES_SQL, args={"bronze_table": bronze_table}).collect()
    if not rows:
        raise RuntimeError(f"{bronze_table} has no pages; nothing to clean")

    accepted, rejected = split_pages(rows, now())
    counts = {"pages": len(rows), "kept": len(accepted)}
    for record in rejected:
        counts[record["reject_reason"]] = counts.get(record["reject_reason"], 0) + 1
    # Many parse errors mean EIA changed its page layout; stop before silver loses those pages.
    if too_many_failures(len(rows), counts.get(PARSE_ERROR, 0)):
        raise RuntimeError(f"Too many pages could not be parsed: {counts}")

    merge_records(spark, silver_table, accepted)
    merge_records(spark, quarantine_table, rejected)
    return counts
