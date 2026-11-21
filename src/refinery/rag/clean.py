"""Checks that decide which parsed EIA pages go into the knowledge base, and why the others do not.

The tag pages list every article with a petroleum tag, so some of them are mostly about natural gas,
electricity or energy in general. An article is kept if its title names a petroleum term,
or its text mentions petroleum terms often enough. STEO pages are petroleum sections, so they skip the topic check.
At the end of each year EIA reposts popular articles ("Today's article was originally published on June 26."),
so a repost is left out: the original is already in the knowledge base.
"""

import re
import unicodedata
from dataclasses import dataclass, replace

from refinery.rag.bronze import ARTICLE
from refinery.rag.sources import ARTICLES_SINCE

MIN_WORDS = 80
MIN_TOPIC_MENTIONS_PER_1000_WORDS = 10.0
PETROLEUM_TERMS = re.compile(
    r"\b(?:crude|oil|petroleum|gasoline|diesel|distillates?|jet fuel|kerosene|refiner(?:y|ies|s|ing)?|"
    r"propane|butane|LPG|NGLs?|hydrocarbon gas liquids|liquid fuels|Brent|WTI|OPEC|barrels?|b/d)\b",
    re.IGNORECASE,
)
REPOST_NOTE = re.compile(r"\boriginally published on\b", re.IGNORECASE)
REPOST_NOTE_WITHIN = 300  # characters from the start of the body, where EIA puts the repost note

# Typographic characters replaced with plain ones, so the text matches what people type in a question
PLAIN_CHARACTERS = str.maketrans(
    {"‘": "'", "’": "'", "‚": "'", "‛": "'", "“": '"', "”": '"', "„": '"', "–": "-", "—": "-", "−": "-"}
)
INVISIBLE_CHARACTERS = re.compile("[\u00ad\u200b\u200c\u200d\u2060\ufeff]")  # soft hyphen, zero-width
SPACES = re.compile(r"[ \t]+")

# Why a page is left out of the knowledge base; the value goes into the quality table
NO_TITLE = "no_title"
TOO_SHORT = "too_short"
PUBLISHED_TOO_EARLY = "published_too_early"
REPOST = "repost"
OFF_TOPIC = "off_topic"


@dataclass(frozen=True)
class PageCheck:
    reject_reason: str | None
    word_count: int
    topic_mentions: int

    @property
    def accepted(self):
        return self.reject_reason is None


def normalize_text(text):
    """Unicode NFKC (non-breaking spaces, ligatures), plain quotes and dashes, no invisible characters."""
    text = INVISIBLE_CHARACTERS.sub("", unicodedata.normalize("NFKC", text).translate(PLAIN_CHARACTERS))
    return "\n".join(SPACES.sub(" ", line).strip() for line in text.split("\n")).strip()


def normalize_page(page):
    """The same parsed page with its title, body and contributors normalized."""
    return replace(
        page,
        title=normalize_text(page.title),
        body=normalize_text(page.body),
        contributors=normalize_text(page.contributors) if page.contributors else page.contributors,
    )


def is_repost(body):
    return REPOST_NOTE.search(body[:REPOST_NOTE_WITHIN]) is not None


def topic_mentions(text):
    return len(PETROLEUM_TERMS.findall(text))


def is_petroleum_topic(title, mentions, word_count):
    if PETROLEUM_TERMS.search(title):
        return True
    return word_count > 0 and mentions * 1000 / word_count >= MIN_TOPIC_MENTIONS_PER_1000_WORDS


def reject_reason(doc_type, page, word_count, mentions, since):
    if not page.title.strip():
        return NO_TITLE
    if word_count < MIN_WORDS:
        return TOO_SHORT
    if doc_type != ARTICLE:
        return None
    if page.published_date < since:
        return PUBLISHED_TOO_EARLY
    if is_repost(page.body):
        return REPOST
    if not is_petroleum_topic(page.title, mentions, word_count):
        return OFF_TOPIC
    return None


def check_page(doc_type, page, since=ARTICLES_SINCE):
    """Whether a parsed page goes into the knowledge base; if not, the reason."""
    word_count = len(page.body.split())
    mentions = topic_mentions(page.body)
    return PageCheck(reject_reason(doc_type, page, word_count, mentions, since), word_count, mentions)
