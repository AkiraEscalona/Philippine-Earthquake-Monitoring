"""Polite HTTP client for the GarbinWx earthquake feed (conditional requests, Retry-After)."""
from __future__ import annotations

import random
import re
from dataclasses import dataclass
from typing import Any

import requests


class FetchError(RuntimeError):
    """Fetch failed. ``retry_after`` (seconds) is set when the server asked us to wait."""

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


@dataclass
class FetchResult:
    status: int  # 200 or 304
    payload: Any = None
    max_age: int | None = None


def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        return None  # HTTP-date form is rare; fall back to our own backoff


def parse_max_age(cache_control: str | None) -> int | None:
    if not cache_control:
        return None
    match = re.search(r"max-age=(\d+)", cache_control)
    return int(match.group(1)) if match else None


def compute_backoff(attempt: int, base: float = 30.0, cap: float = 900.0, rng: random.Random | None = None) -> float:
    """Exponential backoff with +/-20% jitter. ``attempt`` starts at 1."""
    delay = min(cap, base * (2 ** max(attempt - 1, 0)))
    jitter = (rng or random).uniform(0.8, 1.2)
    return min(cap, delay * jitter)


class GarbinWxClient:
    def __init__(self, url: str, user_agent: str, timeout_s: int = 20, session: requests.Session | None = None):
        self.url = url
        self.timeout_s = timeout_s
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        self._etag: str | None = None
        self._last_modified: str | None = None

    def fetch(self) -> FetchResult:
        headers = {}
        if self._etag:
            headers["If-None-Match"] = self._etag
        if self._last_modified:
            headers["If-Modified-Since"] = self._last_modified
        try:
            response = self.session.get(self.url, headers=headers, timeout=self.timeout_s)
        except requests.RequestException as exc:
            raise FetchError(f"network error: {exc}") from exc

        max_age = parse_max_age(response.headers.get("Cache-Control"))
        if response.status_code == 304:
            return FetchResult(304, None, max_age)
        if response.status_code in (429, 503):
            raise FetchError(
                f"HTTP {response.status_code} (rate limited / unavailable)",
                parse_retry_after(response.headers.get("Retry-After")),
            )
        if response.status_code != 200:
            raise FetchError(f"HTTP {response.status_code}", parse_retry_after(response.headers.get("Retry-After")))
        try:
            payload = response.json()
        except ValueError as exc:
            raise FetchError("response was not valid JSON") from exc

        self._etag = response.headers.get("ETag") or self._etag
        self._last_modified = response.headers.get("Last-Modified") or self._last_modified
        return FetchResult(200, payload, max_age)
