import json
from pathlib import Path

import pytest
from assistant.evaluation import EvalQuestionsError, load_questions

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
