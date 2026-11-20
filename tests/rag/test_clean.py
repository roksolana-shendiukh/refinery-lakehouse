from datetime import date
from pathlib import Path

from refinery.rag.bronze import ARTICLE, OUTLOOK
from refinery.rag.clean import (
    NO_TITLE,
    OFF_TOPIC,
    PUBLISHED_TOO_EARLY,
    REPOST,
    TOO_SHORT,
    check_page,
    normalize_page,
    normalize_text,
    topic_mentions,
)
from refinery.rag.parse import ParsedPage, parse_article, parse_steo

FIXTURES = Path(__file__).parent / "fixtures"
GAS_TEXT = "Natural gas storage rose as LNG exports and power demand grew across the country this winter. " * 10


def page(body, title="Natural gas inventories rose", published_date=date(2025, 3, 1)):
    return ParsedPage(title=title, published_date=published_date, body=body)


def test_real_articles_and_steo_page_are_accepted():
    for name in ("tie_article_61142.html", "tie_article_61443.html"):
        assert check_page(ARTICLE, parse_article((FIXTURES / name).read_text(encoding="utf-8"))).accepted
    assert check_page(OUTLOOK, parse_steo((FIXTURES / "steo_global_oil.html").read_text(encoding="utf-8"))).accepted


def test_check_counts_words_and_petroleum_mentions():
    check = check_page(ARTICLE, page("Crude oil and gasoline prices rose, refiners said. " * 20))

    assert check.word_count == 160
    assert check.topic_mentions == 80


def test_topic_terms_match_whole_words_only():
    assert topic_mentions("Toil, soil and boiling water; Brentwood") == 0
    assert topic_mentions("OPEC+ cut output by 2 million b/d; refineries ran at 90%") == 3


def test_article_without_petroleum_terms_is_off_topic():
    assert check_page(ARTICLE, page(GAS_TEXT)).reject_reason == OFF_TOPIC


def test_article_with_a_petroleum_term_in_the_title_is_kept():
    assert check_page(ARTICLE, page(GAS_TEXT, title="Natural gas and crude oil output rose")).accepted


def test_article_that_mentions_petroleum_often_enough_is_kept():
    assert check_page(ARTICLE, page(GAS_TEXT + "Propane exports also rose. " * 2)).accepted


def test_steo_page_skips_the_topic_check():
    assert check_page(OUTLOOK, page(GAS_TEXT, title="Short-Term Energy Outlook: Natural gas")).accepted


def test_short_page_is_rejected():
    assert check_page(ARTICLE, page("Crude oil prices rose. " * 10)).reject_reason == TOO_SHORT


def test_article_published_before_the_knowledge_base_start_is_rejected():
    check = check_page(ARTICLE, page("Crude oil prices rose. " * 30, published_date=date(2023, 12, 29)))

    assert check.reject_reason == PUBLISHED_TOO_EARLY

def test_page_without_a_title_is_rejected():
    assert check_page(ARTICLE, page("Crude oil prices rose. " * 30, title=" ")).reject_reason == NO_TITLE

def test_year_end_repost_is_rejected():
    body = "Today's article was originally published on June 26.\n\n" + "Crude oil prices rose. " * 30

    assert check_page(ARTICLE, page(body)).reject_reason == REPOST


def test_a_mention_of_an_earlier_publication_later_in_the_text_is_not_a_repost():
    body = "Crude oil prices rose. " * 30 + "The data were originally published on March 3."

    assert check_page(ARTICLE, page(body)).accepted


def test_normalize_text_uses_plain_quotes_dashes_and_spaces():
    text = "Russia\u2019s \u201ccrude\u201d  exports\u00a0fell in 2024\u20132025 \u2014 by 5%\u200b. \n\n  Next\tline "

    assert normalize_text(text) == 'Russia\'s "crude" exports fell in 2024-2025 - by 5%.\n\nNext line'


def test_normalize_page_normalizes_title_body_and_contributors():
    parsed = ParsedPage(
        title="Brent\u2019s price", published_date=date(2025, 1, 2), body="Text\u00a0here", contributors="A\u00a0B"
    )

    normalized = normalize_page(parsed)

    assert (normalized.title, normalized.body, normalized.contributors) == ("Brent's price", "Text here", "A B")
    assert normalized.published_date == date(2025, 1, 2)
