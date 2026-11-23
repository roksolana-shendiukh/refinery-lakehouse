"""Evaluation of the assistant: control questions, answers with and without retrieval, scores.

This module holds the logic that needs no MLflow, so it is tested without it and ships with the app
without extra dependencies. The evaluation job (evaluate.py) puts it together with MLflow.
"""

import json
from dataclasses import dataclass
from pathlib import Path

CATEGORIES = ("outlook", "article", "multi_source", "out_of_scope", "injection")
# Span type that MLflow judges read the retrieved passages from (mlflow.entities.SpanType.RETRIEVER)
RETRIEVER_SPAN = "RETRIEVER"


class EvalQuestionsError(ValueError):
    """The questions file does not fit the expected format."""


@dataclass(frozen=True)
class EvalQuestion:
    id: str
    question: str
    category: str
    expected_facts: tuple = ()  # what a correct answer states, checked by the Correctness judge
    expected_sources: tuple = ()  # pages a correct answer cites
    guidelines: tuple = ()  # rules for this question, checked by the guidelines judge
    should_refuse: bool = False  # the right answer is the fixed refusal phrase


def parse_question(raw, position):
    """One question from the JSON file, checked: it must say what a good answer is."""
    if not isinstance(raw, dict):
        raise EvalQuestionsError(f"Question {position} is not an object")
    try:
        question = EvalQuestion(
            id=str(raw["id"]),
            question=str(raw["question"]).strip(),
            category=str(raw["category"]),
            expected_facts=tuple(raw.get("expected_facts", ())),
            expected_sources=tuple(raw.get("expected_sources", ())),
            guidelines=tuple(raw.get("guidelines", ())),
            should_refuse=bool(raw.get("should_refuse", False)),
        )
    except KeyError as error:
        raise EvalQuestionsError(f"Question {position} has no {error.args[0]}") from None

    if not question.question:
        raise EvalQuestionsError(f"Question {question.id} is empty")
    if question.category not in CATEGORIES:
        raise EvalQuestionsError(f"Question {question.id} has unknown category {question.category!r}")
    if not (question.expected_facts or question.guidelines):
        raise EvalQuestionsError(f"Question {question.id} has neither expected_facts nor guidelines")
    if question.should_refuse and question.expected_sources:
        raise EvalQuestionsError(f"Question {question.id} should be refused but expects sources")
    return question


def load_questions(path):
    """The control questions from a JSON list, in file order; ids must be unique."""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise EvalQuestionsError("The questions file must be a non-empty JSON list")
    questions = [parse_question(item, position) for position, item in enumerate(raw, start=1)]
    ids = [question.id for question in questions]
    duplicates = sorted({question_id for question_id in ids if ids.count(question_id) > 1})
    if duplicates:
        raise EvalQuestionsError(f"Duplicate question ids: {duplicates}")
    return questions


def retriever_documents(chunks):
    """Chunks as MLflow retriever documents: the text the answer may rely on, with its page as doc_uri."""
    return [
        {
            "id": chunk.chunk_id,
            "page_content": chunk.text,
            "metadata": {
                "doc_uri": chunk.source_url,
                "title": chunk.title,
                "published_date": chunk.published_date,
                "doc_type": chunk.doc_type,
                "score": chunk.score,
            },
        }
        for chunk in chunks
    ]


class TracedSearch:
    """The article search with every call recorded as a retriever span, for the groundedness judge.
    start_span is mlflow.start_span in the evaluation job; the agent uses this object as it uses ArticleSearch.
    """

    def __init__(self, search, start_span):
        self.search = search
        self.start_span = start_span

    def __call__(self, query, doc_type=None, since=None):
        with self.start_span(name="search_articles", span_type=RETRIEVER_SPAN) as span:
            span.set_inputs({"query": query, "doc_type": doc_type, "since": since.isoformat() if since else None})
            chunks = self.search(query, doc_type=doc_type, since=since)
            span.set_outputs(retriever_documents(chunks))
        return chunks
