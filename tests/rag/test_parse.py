from datetime import date
from pathlib import Path

import pytest

from refinery.rag.parse import PageParseError, parse_article, parse_steo

FIXTURES = Path(__file__).parent / "fixtures"
ARTICLE = (FIXTURES / "tie_article_61142.html").read_text(encoding="utf-8")
WRAPPED_ARTICLE = (FIXTURES / "tie_article_61443.html").read_text(encoding="utf-8")
STEO = (FIXTURES / "steo_global_oil.html").read_text(encoding="utf-8")
TAG_PAGE = (FIXTURES / "tie_tag_page.html").read_text(encoding="utf-8")


def small_article(body, contributor='<p><strong>Principal contributor: </strong>EIA Staff</p>'):
    return (
        '<div class="tie-article"><span class="date">May 1, 2026</span>'
        f'<h1><a href="#">Title</a></h1>{body}{contributor}</div>'
    )


def test_parse_article_reads_title_date_contributor_and_tags():
    page = parse_article(ARTICLE)

    assert page.title == "Brent crude oil prices averaged $19 per barrel less in 2023 than 2022"
    assert page.published_date == date(2024, 1, 2)
    assert page.contributors == "Matthew French"
    assert page.tags == ("Brent", "crude oil", "oil/petroleum", "liquid fuels", "prices", "international", "OPEC")


def test_parse_article_keeps_the_text_from_the_first_chart_to_the_last_paragraph():
    body = parse_article(ARTICLE).body

    assert body.startswith("[Chart: Brent crude oil daily spot price]")
    assert "Data source: Bloomberg, L.P.\nNote: Spot prices are in nominal terms" in body
    assert body.endswith("pushed the price of Brent up to $78/b on December 29 (the last trading day of the year).")


def test_parse_article_drops_page_furniture():
    body = parse_article(ARTICLE).body

    for text in ("In-brief analysis", "Principal contributor", "Tags:", "More recent articles", "About EIA", "Glossary"):
        assert text not in body


def test_parse_article_joins_link_text_without_extra_spaces():
    body = parse_article(ARTICLE).body

    assert "Russia’s crude oil and products, several interest rate hikes" in body


def test_parse_article_turns_a_chart_inside_a_paragraph_into_a_chart_line():
    page = parse_article(small_article('<p>Before <img src="images/a.svg" alt="Refinery   runs"> after.</p>'))

    assert page.body == "Before [Chart: Refinery runs] after."


def test_parse_article_reads_text_wrapped_in_an_unclosed_paragraph():
    page = parse_article(WRAPPED_ARTICLE)

    assert page.title == "U.S. energy flows through Panama Canal rose slightly in January"
    assert page.published_date == date(2024, 2, 21)
    assert page.contributors == "Josh Eiermann"
    assert page.body.startswith("U.S. liquefied petroleum gas (LPG) and ethane exports through the Panama Canal rose")
    assert "[Chart: monthly U.S. petroleum product exports via the Panama Canal]" in page.body
    assert page.body.endswith("and VLGC rates are the lowest since 2022.")
    for text in ("Principal contributor", "Tags:", "More recent articles"):
        assert text not in page.body


def test_parse_article_fails_on_a_tag_page():
    with pytest.raises(PageParseError, match="No article"):
        parse_article(TAG_PAGE)


def test_parse_article_fails_without_a_contributor_line():
    with pytest.raises(PageParseError, match="Principal contributor"):
        parse_article(small_article("<p>Text</p>", contributor=""))


def test_parse_steo_reads_section_title_and_release_dates():
    page = parse_steo(STEO)

    assert page.title == "Short-Term Energy Outlook: Global oil markets"
    assert page.published_date == date(2026, 11, 10)
    assert page.next_release_date == date(2026, 12, 8)


def test_parse_steo_keeps_subheadings_on_their_own_line():
    assert parse_steo(STEO).body.startswith("Global oil prices\nThe Brent crude oil spot price increased")


def test_parse_steo_keeps_charts_and_drops_related_links():
    body = parse_steo(STEO).body

    assert "[Chart: Estimated Strait of Hormuz closure-related disruptions in crude oil production]" in body
    for text in ("Related Tables", "Related Figures", "Interactive Data Viewers", "Next Release Date"):
        assert text not in body


def test_parse_steo_adds_the_summary_table_with_forecast_years_marked():
    body = parse_steo(STEO).body

    assert "Global Petroleum and Other Liquids:" in body
    assert (
        "Brent crude oil spot price (dollars per barrel): 2024: 81; 2025: 69; 2026 (forecast): 97; 2027 (forecast): 86"
        in body
    )


def test_parse_steo_fails_without_a_release_date():
    with pytest.raises(PageParseError, match="release date"):
        parse_steo('<div class="main_col"><h1>Global oil markets</h1><p>Text</p></div>')
