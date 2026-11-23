from assistant.layout import (
    ANSWER,
    ASK,
    CONTEXT,
    DETAILS,
    EXAMPLE,
    EXAMPLE_QUESTIONS,
    QUESTION,
    SOURCES,
    STATUS,
    not_configured,
    page,
)


def component_ids(component):
    """Every id in a Dash component tree."""
    ids = []
    if getattr(component, "id", None) is not None:
        ids.append(component.id)
    children = getattr(component, "children", None)
    for child in children if isinstance(children, list | tuple) else [children]:
        if hasattr(child, "to_plotly_json"):
            ids.extend(component_ids(child))
    return ids


def test_page_has_every_component_the_callbacks_use():
    ids = component_ids(page())

    for component_id in (QUESTION, ASK, STATUS, ANSWER, SOURCES, CONTEXT, DETAILS):
        assert component_id in ids


def test_page_offers_the_example_questions_as_buttons():
    examples = [i for i in component_ids(page()) if isinstance(i, dict) and i["type"] == EXAMPLE]

    assert [example["index"] for example in examples] == list(range(len(EXAMPLE_QUESTIONS)))


def test_not_configured_page_shows_the_error():
    assert "ASSISTANT_LLM_ENDPOINT is not set" in str(not_configured("ASSISTANT_LLM_ENDPOINT is not set"))
