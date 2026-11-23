from types import SimpleNamespace

from assistant.agent import AgentAnswer
from assistant.callbacks import (
    EMPTY_QUESTION,
    answer_question,
    chosen_question,
    index_available,
)
from assistant.layout import ANSWER, DETAILS, EXAMPLE, EXAMPLE_QUESTIONS, SOURCES, STATUS
from assistant.views import SWITCHED_OFF
from databricks.sdk.errors import NotFound

ANSWER_TEXT = "About $100/b [STEO](https://www.eia.gov/outlooks/steo/report/global_oil.php)."


class FakeAgent:
    def __init__(self):
        self.questions = []

    def ask(self, question):
        self.questions.append(question)
        return AgentAnswer(question=question, answer=ANSWER_TEXT, model="m", iterations=1, latency_ms=1200)


class FakeLog:
    def __init__(self):
        self.writes = []

    def write(self, answer, user_email=None, request_id=None):
        self.writes.append((answer.question, user_email, request_id))


def now(function, *args):
    function(*args)


def test_question_goes_to_the_agent_and_is_logged_with_the_viewer():
    agent, log = FakeAgent(), FakeLog()

    shown = answer_question("  Brent forecast?  ", agent, log, lambda: True, "analyst@example.com", background=now)

    assert agent.questions == ["Brent forecast?"]
    assert shown[ANSWER].children == ANSWER_TEXT
    assert shown[STATUS] is None
    ((question, email, request_id),) = log.writes
    assert (question, email) == ("Brent forecast?", "analyst@example.com")
    assert shown[DETAILS].endswith(f"request {request_id}")


def test_empty_question_asks_for_one_and_calls_nothing():
    agent, log = FakeAgent(), FakeLog()

    shown = answer_question("   ", agent, log, lambda: True, background=now)

    assert shown[STATUS] == EMPTY_QUESTION
    assert (agent.questions, log.writes, shown[SOURCES]) == ([], [], [])


def test_without_the_index_the_page_says_the_assistant_is_switched_off():
    agent = FakeAgent()

    shown = answer_question("Brent forecast?", agent, FakeLog(), lambda: False, background=now)

    assert SWITCHED_OFF in str(shown[STATUS])
    assert agent.questions == []


def test_example_button_asks_its_question():
    assert chosen_question({"type": EXAMPLE, "index": 1}, "typed text") == EXAMPLE_QUESTIONS[1]
    assert chosen_question("ask", "typed text") == "typed text"


def test_index_is_available_only_when_it_exists():
    def get_index(name):
        raise NotFound(f"{name} not found")

    missing = SimpleNamespace(vector_search_indexes=SimpleNamespace(get_index=get_index))
    present = SimpleNamespace(vector_search_indexes=SimpleNamespace(get_index=lambda name: object()))

    assert not index_available(missing, "c.rag.eia_article_chunks_index")
    assert index_available(present, "c.rag.eia_article_chunks_index")
