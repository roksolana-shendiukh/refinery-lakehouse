from datetime import UTC, date, datetime
from pathlib import Path

from rag_testing import terraform_columns

from refinery.rag.chunking import chunk_header, chunk_records, page_chunks, split_text, word_count
from refinery.rag.clean import normalize_page
from refinery.rag.parse import parse_article

FIXTURES = Path(__file__).parent / "fixtures"


def paragraph(number, words=40):
    """A paragraph of 4 sentences that tells which paragraph it is: 'P3 s1 w w ... w.'."""
    sentence = " ".join(["w"] * (words // 4 - 2))
    return " ".join(f"P{number} s{index} {sentence}." for index in range(1, 5))


def silver_page(body, doc_id="article_61142"):
    return {
        "doc_id": doc_id,
        "doc_type": "article",
        "title": "Brent crude oil prices fell",
        "published_date": date(2024, 1, 2),
        "source_url": "https://www.eia.gov/todayinenergy/detail.php?id=61142",
        "body_text": body,
    }


def test_short_body_is_one_chunk():
    body = "First paragraph.\n\nSecond paragraph."

    assert split_text(body) == [body]


def test_chunks_are_whole_paragraphs_within_the_limit():
    body = "\n\n".join(paragraph(number) for number in range(1, 11))

    chunks = split_text(body, max_words=100, overlap_words=0)

    assert all(word_count(chunk) <= 100 for chunk in chunks)
    assert chunks[0] == f"{paragraph(1)}\n\n{paragraph(2)}"
    assert "\n\n".join(chunks) == body


def test_next_chunk_starts_with_the_last_sentences_of_the_previous_one():
    body = "\n\n".join(paragraph(number) for number in range(1, 7))

    chunks = split_text(body, max_words=100, overlap_words=25)

    assert chunks[1].startswith("P2 s3 ")
    assert all(word_count(chunk) <= 100 for chunk in chunks)


def test_long_paragraph_is_split_by_sentences():
    chunks = split_text(paragraph(1, words=400), max_words=100, overlap_words=0)

    assert len(chunks) == 4
    assert [chunk.split()[1] for chunk in chunks] == ["s1", "s2", "s3", "s4"]


def test_sentence_longer_than_the_limit_is_cut_by_words():
    chunks = split_text(" ".join(["word"] * 250), max_words=100, overlap_words=0)

    assert [word_count(chunk) for chunk in chunks] == [100, 100, 50]


def test_chunk_header_has_title_and_readable_date():
    assert (
        chunk_header("Brent crude oil prices fell", date(2024, 1, 2)) == "Brent crude oil prices fell (January 2, 2024)"
    )


def test_page_chunks_have_stable_ids_metadata_and_the_header():
    body = "\n\n".join(paragraph(number) for number in range(1, 11))

    chunks = page_chunks(silver_page(body), max_words=100, overlap_words=0)

    assert [chunk["chunk_id"] for chunk in chunks[:2]] == ["article_61142#0000", "article_61142#0001"]
    assert [chunk["chunk_index"] for chunk in chunks] == list(range(len(chunks)))
    assert chunks[1]["chunk_text"].startswith("Brent crude oil prices fell (January 2, 2024)\n\nP3 s1 ")
    assert chunks[1]["published_date"] == date(2024, 1, 2)
    assert page_chunks(silver_page(body), max_words=100, overlap_words=0) == chunks


def test_real_article_is_chunked_without_losing_text():
    page = normalize_page(parse_article((FIXTURES / "tie_article_61443.html").read_text(encoding="utf-8")))

    chunks = split_text(page.body)

    assert len(chunks) == 5
    assert all(word_count(chunk) <= 300 for chunk in chunks)
    for block in page.body.split("\n\n"):
        assert any(block in chunk for chunk in chunks)


def test_chunk_records_have_exactly_the_columns_of_the_terraform_table():
    processed_at = datetime(2026, 11, 21, 9, 0, tzinfo=UTC)
    pages = [silver_page("Short text."), silver_page("Another page.", doc_id="outlook_global_oil")]

    records = chunk_records(pages, processed_at)

    assert [record["chunk_id"] for record in records] == ["article_61142#0000", "outlook_global_oil#0000"]
    assert all(set(record) == set(terraform_columns("eia_article_chunks")) for record in records)
    assert all(record["processed_at"] == processed_at for record in records)
