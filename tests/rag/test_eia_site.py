import pytest
import requests

from refinery.rag.eia_site import USER_AGENT, EiaClient, PageNotFound


class FakeResponse:
    def __init__(self, status_code=200, text="<html></html>", headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}

    @property
    def is_redirect(self):
        return self.status_code in (301, 302, 303, 307, 308)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    """Returns the planned responses in order (an exception in the plan is raised) and records every call."""

    def __init__(self, *planned):
        self.planned = list(planned)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        outcome = self.planned.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class FakeClock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def make_client(session, clock=None, **options):
    clock = clock or FakeClock()
    return EiaClient(session=session, sleep=clock.sleep, clock=clock.time, **options)


def test_get_text_returns_the_page():
    client = make_client(FakeSession(FakeResponse(text="<p>article</p>")))

    assert client.get_text("https://www.eia.gov/todayinenergy/detail.php?id=1") == "<p>article</p>"


def test_get_text_sends_user_agent_cache_buster_and_no_redirects():
    session = FakeSession(FakeResponse())
    client = make_client(session)

    client.get_text("https://www.eia.gov/todayinenergy/index.php?tg=gasoline")

    _, kwargs = session.calls[0]
    assert kwargs["headers"]["User-Agent"] == USER_AGENT
    assert kwargs["params"] == {"nocache": client.cache_buster}
    assert kwargs["allow_redirects"] is False


def test_get_text_raises_page_not_found_on_redirect():
    client = make_client(FakeSession(FakeResponse(302, headers={"Location": "https://www.eia.gov/todayinenergy/"})))

    with pytest.raises(PageNotFound, match="redirects to https://www.eia.gov/todayinenergy/"):
        client.get_text("https://www.eia.gov/todayinenergy/detail.php?id=99999999")


def test_get_text_waits_between_requests():
    clock = FakeClock()
    client = make_client(FakeSession(FakeResponse(), FakeResponse()), clock, delay_seconds=1.5)

    client.get_text("https://www.eia.gov/a")
    client.get_text("https://www.eia.gov/b")

    assert clock.sleeps == [1.5]


def test_get_text_does_not_wait_when_enough_time_has_passed():
    clock = FakeClock()
    client = make_client(FakeSession(FakeResponse(), FakeResponse()), clock, delay_seconds=1.5)

    client.get_text("https://www.eia.gov/a")
    clock.now += 10
    client.get_text("https://www.eia.gov/b")

    assert clock.sleeps == []


def test_get_text_retries_server_errors_with_growing_pauses():
    clock = FakeClock()
    session = FakeSession(FakeResponse(503), FakeResponse(502), FakeResponse(text="ok"))
    client = make_client(session, clock, delay_seconds=0, retries=3, backoff_seconds=5)

    assert client.get_text("https://www.eia.gov/a") == "ok"
    assert clock.sleeps == [5, 10]


def test_get_text_follows_retry_after_on_429():
    clock = FakeClock()
    session = FakeSession(FakeResponse(429, headers={"Retry-After": "30"}), FakeResponse(text="ok"))
    client = make_client(session, clock, delay_seconds=0)

    assert client.get_text("https://www.eia.gov/a") == "ok"
    assert clock.sleeps == [30]


def test_get_text_retries_connection_errors():
    session = FakeSession(requests.ConnectionError("reset"), FakeResponse(text="ok"))
    client = make_client(session, delay_seconds=0)

    assert client.get_text("https://www.eia.gov/a") == "ok"


def test_get_text_raises_after_the_last_retry():
    session = FakeSession(FakeResponse(503), FakeResponse(503), FakeResponse(503))
    client = make_client(session, delay_seconds=0, retries=3)

    with pytest.raises(requests.HTTPError, match="503"):
        client.get_text("https://www.eia.gov/a")
    assert len(session.calls) == 3


def test_get_text_does_not_retry_client_errors():
    session = FakeSession(FakeResponse(404))
    client = make_client(session, delay_seconds=0)

    with pytest.raises(requests.HTTPError, match="404"):
        client.get_text("https://www.eia.gov/a")
    assert len(session.calls) == 1
