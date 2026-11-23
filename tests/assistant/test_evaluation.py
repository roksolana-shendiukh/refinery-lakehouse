import datetime
import json
from contextlib import contextmanager
from pathlib import Path

import pytest
from assistant.evaluation import (
    RETRIEVER_SPAN,
    EvalQuestionsError,
    TracedSearch,
    load_questions,
    retriever_documents,
)
from assistant.retrieval import Chunk

QUESTIONS_FILE = Path(__file__).parents[2] / "data" / "rag" / "eval_questions.json"


def write_questions(tmp_path, questions):
    path = tmp_path / "questions.json"
    path.write_text(json.dumps(questions), encoding="utf-8")
    return path


def test_repository_questions_are_valid_and_cover_every_kind():
    questions = load_questions(QUESTIONS_FILE)

    categories = {question.category for question in questions}
    assert categories == {"outlook", "article", "multi_source", "out_of_scope", "injection"}
    assert any(question.should_refuse for question in questions)
    for question in questions:
        if question.expected_facts:
            assert question.expected_sources, f"{question.id} has facts but no source to cite"
        for url in question.expected_sources:
            assert url.startswith("https://www.eia.gov/"), url


def test_question_without_facts_or_guidelines_is_rejected(tmp_path):
    path = write_questions(tmp_path, [{"id": "q1", "question": "Brent?", "category": "outlook"}])

    with pytest.raises(EvalQuestionsError, match="neither expected_facts nor guidelines"):
        load_questions(path)


def test_refused_question_cannot_expect_sources(tmp_path):
    path = write_questions(
        tmp_path,
        [
            {
                "id": "q1",
                "question": "Borscht?",
                "category": "out_of_scope",
                "should_refuse": True,
                "guidelines": ["No recipe."],
                "expected_sources": ["https://www.eia.gov/x"],
            }
        ],
    )

    with pytest.raises(EvalQuestionsError, match="should be refused"):
        load_questions(path)


def test_duplicate_ids_are_rejected(tmp_path):
    question = {"id": "q1", "question": "Brent?", "category": "outlook", "expected_facts": ["$97/b"]}
    path = write_questions(tmp_path, [question, question])

    with pytest.raises(EvalQuestionsError, match="Duplicate question ids"):
        load_questions(path)


def test_missing_field_is_named(tmp_path):
    path = write_questions(tmp_path, [{"id": "q1", "category": "outlook", "expected_facts": ["$97/b"]}])

    with pytest.raises(EvalQuestionsError, match="has no question"):
        load_questions(path)


CHUNK = Chunk(
    chunk_id="steo-global-oil-0",
    doc_id="steo-global-oil",
    doc_type="outlook",
    title="Short-Term Energy Outlook: Global oil markets",
    published_date="2026-11-10",
    source_url="https://www.eia.gov/outlooks/steo/report/global_oil.php",
    text="Brent averages $97/b in 2026.",
    score=0.82,
)


class FakeSpan:
    def __init__(self, name, span_type):
        self.name, self.span_type = name, span_type
        self.inputs = self.outputs = None

    def set_inputs(self, inputs):
        self.inputs = inputs

    def set_outputs(self, outputs):
        self.outputs = outputs


def span_recorder():
    spans = []

    @contextmanager
    def start_span(name, span_type):
        span = FakeSpan(name, span_type)
        spans.append(span)
        yield span

    return spans, start_span


def test_retrieved_chunks_become_documents_with_their_page():
    (document,) = retriever_documents([CHUNK])

    assert document["id"] == CHUNK.chunk_id
    assert document["page_content"] == CHUNK.text
    assert document["metadata"]["doc_uri"] == CHUNK.source_url
    assert document["metadata"]["published_date"] == "2026-11-10"


def test_every_search_is_a_retriever_span_and_the_agent_gets_the_chunks():
    calls = []

    def search(query, doc_type=None, since=None):
        calls.append((query, doc_type, since))
        return [CHUNK]

    spans, start_span = span_recorder()
    traced = TracedSearch(search, start_span)

    chunks = traced("Brent forecast", doc_type="outlook", since=datetime.date(2026, 1, 1))

    assert chunks == [CHUNK]
    assert calls == [("Brent forecast", "outlook", datetime.date(2026, 1, 1))]
    (span,) = spans
    assert (span.name, span.span_type) == ("search_articles", RETRIEVER_SPAN)
    assert span.inputs == {"query": "Brent forecast", "doc_type": "outlook", "since": "2026-01-01"}
    assert span.outputs == retriever_documents([CHUNK])


def test_empty_search_is_recorded_as_no_documents():
    spans, start_span = span_recorder()

    assert TracedSearch(lambda query, doc_type=None, since=None: [], start_span)("borscht") == []
    assert spans[0].outputs == []
