"""Polite HTTP client for eia.gov: a descriptive User-Agent, a pause between requests, retries and redirect checks."""

import logging
import time

import requests

logger = logging.getLogger(__name__)

USER_AGENT = "refinery-lakehouse/0.1 (Databricks internship project; RAG assistant on EIA articles)"
RETRY_STATUSES = {429, 500, 502, 503, 504}


class PageNotFound(Exception):
    """eia.gov redirects a missing article (unknown detail.php id) to the Today in Energy home page."""


class EiaClient:
    def __init__(
        self,
        session=None,
        delay_seconds=1.5,
        retries=3,
        backoff_seconds=5.0,
        timeout_seconds=30,
        sleep=time.sleep,
        clock=time.monotonic,
    ):
        self.session = session or requests.Session()
        self.delay_seconds = delay_seconds
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.timeout_seconds = timeout_seconds
        self.sleep = sleep
        self.clock = clock
        self.cache_buster = str(int(time.time()))
        self._last_request = None

    def get_text(self, url):
        """HTML of the page. Raises PageNotFound on a redirect and requests errors after the last retry."""
        for attempt in range(1, self.retries + 1):
            self._wait_between_requests()
            try:
                response = self.session.get(
                    url,
                    params={"nocache": self.cache_buster},
                    headers={"User-Agent": USER_AGENT, "Cache-Control": "no-cache"},
                    timeout=self.timeout_seconds,
                    allow_redirects=False,
                )
            except (requests.ConnectionError, requests.Timeout) as error:
                if attempt == self.retries:
                    raise
                self._back_off(url, attempt, f"{type(error).__name__}")
                continue

            if response.is_redirect:
                raise PageNotFound(f"{url} redirects to {response.headers.get('Location')}")
            if response.status_code in RETRY_STATUSES and attempt < self.retries:
                self._back_off(url, attempt, f"HTTP {response.status_code}", response.headers.get("Retry-After"))
                continue
            response.raise_for_status()
            return response.text
        raise AssertionError("unreachable")

    def _wait_between_requests(self):
        if self._last_request is not None:
            remaining = self.delay_seconds - (self.clock() - self._last_request)
            if remaining > 0:
                self.sleep(remaining)
        self._last_request = self.clock()

    def _back_off(self, url, attempt, reason, retry_after=None):
        if retry_after and retry_after.isdigit():
            seconds = int(retry_after)
        else:
            seconds = self.backoff_seconds * attempt
        logger.warning("%s for %s, retry %d of %d in %s s", reason, url, attempt, self.retries - 1, seconds)
        self.sleep(seconds)
