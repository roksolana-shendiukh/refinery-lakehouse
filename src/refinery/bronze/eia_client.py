"""HTTP client for the EIA API: the only place that knows the API key."""

import time

import requests

RETRY_STATUSES = {429, 500, 502, 503, 504}


class EiaClient:
    def __init__(
        self,
        api_key,
        session=None,
        retries=3,
        backoff_seconds=5,
        timeout_seconds=60,
        sleep=time.sleep,
    ):
        self.api_key = api_key
        self.session = session or requests.Session()
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.timeout_seconds = timeout_seconds
        self.sleep = sleep

    def get_json(self, url, params):
        """GET one page; retry temporary failures; never put the key in an error."""
        params = params + [("api_key", self.api_key)]
        status = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)
                status = response.status_code
            except requests.RequestException:
                status = "no response"

            if status == 200:
                return response.json()
            if status != "no response" and status not in RETRY_STATUSES:
                break
            if attempt < self.retries:
                self.sleep(self.backoff_seconds * attempt)

        raise RuntimeError(f"EIA request to {url} failed after {attempt} attempt(s): {status}")
