from assistant.agent import AgentAnswer, ToolCall
from assistant.prompts import Source
from assistant.retrieval import Chunk
from assistant.views import (
    SWITCHED_OFF,
    answer_view,
    context_view,
    details_view,
    readable_date,
    sources_view,
    switched_off_view,
)

STEO = Source(
    "Short-Term Energy Outlook: Global oil markets",
    "2026-11-10",
    "https://www.eia.gov/outlooks/steo/report/global_oil.php",
    "outlook",
    cited=True,
)
ARTICLE = Source(
    "Brent prices", "2024-01-02", "https://www.eia.gov/todayinenergy/detail.php?id=61142", "article", False
)
CHUNK = Chunk(
    "outlook_global_oil#0001", "outlook_global_oil", "outlook", STEO.title, "2026-11-10", STEO.url, "Brent...", 0.95
)
ANSWER = AgentAnswer(
    question="Brent forecast?",
    answer=f"About $100/b [STEO]({STEO.url}).",
    model="m",
    sources=(STEO, ARTICLE),
    chunks=(CHUNK,),
    tool_calls=(ToolCall("search_articles", "{}", (CHUNK.chunk_id,)),),
    iterations=2,
    prompt_tokens=4731,
    completion_tokens=169,
    latency_ms=5578,
)


def test_dates_are_shown_as_month_day_year():
    assert readable_date("2026-11-10") == "Nov 10, 2026"
    assert readable_date("") == ""


def test_answer_is_markdown_with_links_in_a_new_tab():
    view = answer_view(ANSWER)

    assert view.children == ANSWER.answer
    assert view.link_target == "_blank"


def test_cited_sources_come_first_and_the_rest_is_folded():
    heading, cited, folded = sources_view(ANSWER.sources)

    assert heading.children == "Sources"
    assert "STEO" in str(cited) and STEO.url in str(cited) and "Nov 10, 2026" in str(cited)
    assert "Also found by the search (1)" in str(folded) and "Today in Energy" in str(folded)


def test_sources_without_a_citation_are_all_folded():
    (folded,) = sources_view((ARTICLE,))

    assert "not cited in the answer" in str(folded)


def test_no_sources_no_section():
    assert sources_view(()) == []


def test_context_lists_the_passages_the_model_read():
    view = context_view(ANSWER.chunks)

    assert "Retrieved context (1 passage)" in str(view)
    assert "score 0.95" in str(view)
    assert "Brent..." in str(view)


def test_details_tell_calls_searches_tokens_time_and_request():
    assert details_view(ANSWER, "r-1") == "2 model calls · 1 search · 4,900 tokens · 5.6 s · request r-1"


def test_switched_off_view_points_to_the_start_workflow():
    assert SWITCHED_OFF in str(switched_off_view())
    assert "assistant-start" in SWITCHED_OFF
