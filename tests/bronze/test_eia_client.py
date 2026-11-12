import pytest
import requests

from refinery.bronze.eia_client import EiaClient

URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
KEY = "secret-test-key"


class FakeResponse:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self.body = body

    def json(self):
        return self.body


class FakeSession:
    def __init__(self, answers):
        self.answers = list(answers)
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def make_client(answers):
    session = FakeSession(answers)
    sleeps = []
    client = EiaClient(KEY, session=session, sleep=sleeps.append)
    return client, session, sleeps


def test_get_json_returns_body_on_success():
    client, session, sleeps = make_client([FakeResponse(200, {"response": {"total": "0"}})])

    body = client.get_json(URL, [("frequency", "weekly")])

    assert body == {"response": {"total": "0"}}
    assert len(session.calls) == 1
    assert sleeps == []


def test_get_json_adds_key_only_when_sending():
    client, session, _ = make_client([FakeResponse(200, {})])
    params = [("frequency", "weekly")]

    client.get_json(URL, params)

    assert session.calls[0]["params"] == [("frequency", "weekly"), ("api_key", KEY)]
    assert params == [("frequency", "weekly")]


def test_get_json_always_sets_a_timeout():
    client, session, _ = make_client([FakeResponse(200, {})])

    client.get_json(URL, [])

    assert session.calls[0]["timeout"] == 60


def test_get_json_retries_temporary_error_then_succeeds():
    client, session, sleeps = make_client([FakeResponse(503), FakeResponse(200, {"ok": True})])

    body = client.get_json(URL, [])

    assert body == {"ok": True}
    assert len(session.calls) == 2
    assert sleeps == [5]


def test_get_json_retries_when_network_fails():
    client, session, _ = make_client([requests.ConnectionError("no internet"), FakeResponse(200, {"ok": True})])

    body = client.get_json(URL, [])

    assert body == {"ok": True}
    assert len(session.calls) == 2


def test_get_json_does_not_retry_permanent_error():
    client, session, sleeps = make_client([FakeResponse(403)])

    with pytest.raises(RuntimeError):
        client.get_json(URL, [])

    assert len(session.calls) == 1
    assert sleeps == []


def test_get_json_gives_up_after_all_retries():
    client, session, sleeps = make_client([FakeResponse(503)] * 3)

    with pytest.raises(RuntimeError):
        client.get_json(URL, [])

    assert len(session.calls) == 3
    assert sleeps == [5, 10]


def test_get_json_error_never_shows_the_key():
    client, _, _ = make_client([requests.ConnectionError(f"failed {URL}?api_key={KEY}")] * 3)

    with pytest.raises(RuntimeError) as error:
        client.get_json(URL, [])

    assert KEY not in str(error.value)
    assert URL in str(error.value)
