"""Reads the text out of landed EIA pages: a Today in Energy article or a STEO report section.

Only the article or section itself is kept; site navigation, footers and "related" blocks are dropped.
Charts become "[Chart: <alt text>]" lines, because the chart data itself is an image.
"""

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime

from bs4 import BeautifulSoup, NavigableString, Tag

DATE_FORMAT = "%B %d, %Y"  # e.g. "January 2, 2024"
DATE_TEXT = r"[A-Z][a-z]+ \d{1,2}, \d{4}"
SKIPPED_TAGS = {"script", "style", "hr"}
LINE_BREAK = "\x00"  # stands in for <br> while whitespace from the HTML source is collapsed
SKIPPED_CLASSES = {"no_print", "do-not-print"}
CONTRIBUTOR = re.compile(r"Principal contributors?:\s*(\S.*)", re.IGNORECASE)
WHITESPACE = re.compile(r"\s+")


@dataclass(frozen=True)
class ParsedPage:
    title: str
    published_date: date
    body: str
    contributors: str | None = None
    tags: tuple = ()
    next_release_date: date | None = None


class PageParseError(ValueError):
    """The page does not have the expected structure (EIA changed the layout, or it is not an article)."""


def parse_date(text):
    return datetime.strptime(text.strip(), DATE_FORMAT).replace(tzinfo=UTC).date()


def collapse(text):
    return WHITESPACE.sub(" ", text).strip()


def chart_line(img):
    alt = collapse(img.get("alt", ""))
    return f"[Chart: {alt}]" if alt else "[Chart]"


def block_text(element):
    """Text of one block: charts become [Chart: ...], <br> becomes a line break, other HTML whitespace one space."""
    if element.name == "img":
        return chart_line(element)
    for img in element.find_all("img"):
        img.replace_with(NavigableString(f" {chart_line(img)} "))
    for line_break in element.find_all("br"):
        line_break.replace_with(NavigableString(LINE_BREAK))
    lines = collapse(element.get_text()).split(LINE_BREAK)
    return "\n".join(line.strip() for line in lines if line.strip())


def is_skipped(element):
    return element.name in SKIPPED_TAGS or bool(SKIPPED_CLASSES & set(element.get("class", [])))


def body_blocks(elements):
    """Non-empty text blocks of the given sibling elements, in page order."""
    blocks = []
    for element in elements:
        if isinstance(element, NavigableString):
            text = collapse(str(element))
        elif isinstance(element, Tag) and not is_skipped(element):
            text = block_text(element)
        else:
            continue
        if text:
            blocks.append(text)
    return blocks


def has_contributor_line(element):
    return isinstance(element, Tag) and CONTRIBUTOR.search(element.get_text(" ")) is not None


def split_at_contributor(elements):
    """Elements before the 'Principal contributor' line, and the contributors named in it."""
    body = []
    for element in elements:
        if not has_contributor_line(element):
            body.append(element)
        elif any(has_contributor_line(child) for child in element.children):
            inner_body, contributors = split_at_contributor(element.children)
            return body + inner_body, contributors
        else:
            return body, collapse(CONTRIBUTOR.search(element.get_text(" ")).group(1))
    return body, None


def parse_article(html):
    """Title, date, body, contributors and tags of a Today in Energy article page."""
    soup = BeautifulSoup(html, "html.parser")
    article = soup.select_one("div.tie-article:not(.tag-view)")
    date_span = article.select_one("span.date") if article else None
    heading = date_span.find_next("h1") if date_span else None
    if heading is None:
        raise PageParseError("No article with a date and a title (div.tie-article, span.date, h1)")

    body_elements, contributors = split_at_contributor(heading.next_siblings)
    if contributors is None:
        raise PageParseError("No 'Principal contributor' line after the article text")

    tags = tuple(
        collapse(link.get_text()) for link in article.select("div.do-not-print a[href*='tg=']") if link.get_text()
    )
    return ParsedPage(
        title=collapse(heading.get_text(" ")),
        published_date=parse_date(date_span.get_text()),
        body="\n\n".join(body_blocks(body_elements)),
        contributors=contributors,
        tags=tags,
    )


def summary_table_text(table):
    """STEO summary table as text lines: 'Brent crude oil spot price (dollars per barrel): 2024: 81; ...'."""
    caption = collapse(table.select_one("thead tr th").get_text(" "))
    years = []
    for header in table.select("thead tr")[-1].find_all("th")[1:]:
        hidden = header.select_one("span.hidden")
        forecast = hidden is not None and "projected" in hidden.get_text()
        if hidden:
            hidden.decompose()
        years.append(collapse(header.get_text()) + (" (forecast)" if forecast else ""))

    lines = [f"{caption}:"]
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) != len(years) + 1:
            continue
        label = collapse(cells[0].get_text(" "))
        values = "; ".join(f"{year}: {collapse(cell.get_text())}" for year, cell in zip(years, cells[1:], strict=True))
        lines.append(f"{label}: {values}")
    return "\n".join(lines)


def parse_steo(html):
    """Section title, release dates and text of a STEO report section page, plus its summary table."""
    soup = BeautifulSoup(html, "html.parser")
    header = soup.select_one("div.pub_title")
    section = soup.select_one("div.main_col")
    heading = section.find("h1") if section else None
    release = re.search(rf"Release Date:\s*({DATE_TEXT})", collapse(header.get_text(" "))) if header else None
    if heading is None or release is None:
        raise PageParseError("No STEO release date (div.pub_title) or section (div.main_col h1)")

    next_release = re.search(rf"Next Release Date:\s*({DATE_TEXT})", collapse(header.get_text(" ")))
    blocks = body_blocks(heading.next_siblings)
    table = soup.select_one("table.steoPriceSummary")
    if table is not None:
        blocks.append(summary_table_text(table))

    return ParsedPage(
        title=f"Short-Term Energy Outlook: {collapse(heading.get_text(' '))}",
        published_date=parse_date(release.group(1)),
        body="\n\n".join(blocks),
        next_release_date=parse_date(next_release.group(1)) if next_release else None,
    )
