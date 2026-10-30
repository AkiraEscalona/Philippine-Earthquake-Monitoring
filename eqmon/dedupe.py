"""Producer-side change detection so unchanged earthquakes are not re-published."""
from __future__ import annotations

import hashlib
import json
import os
from typing import Iterable

_FINGERPRINT_FIELDS = ("event_id", "magnitude", "depth_km", "latitude", "longitude", "location", "bulletin_url")


def fingerprint(event: dict) -> str:
    payload = json.dumps([event.get(f) for f in _FINGERPRINT_FIELDS], sort_keys=True, default=str)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


class Deduper:
    """Tracks the last published fingerprint per event_id.

    * Within one feed, repeated event_ids are dropped (first occurrence wins).
    * Across polls, only new or revised events pass.
    * An event is only remembered after ``commit`` - i.e. after Kafka confirmed delivery.
    """

    def __init__(self, state_path: str | None = None, max_entries: int = 200_000):
        self._seen: dict[str, str] = {}
        self._path = state_path
        self._max = max_entries
        if state_path:
            self._load()

    def __len__(self) -> int:
        return len(self._seen)

    def filter_new(self, events: Iterable[dict]) -> tuple[list[dict], int]:
        fresh: list[dict] = []
        in_batch: set[str] = set()
        duplicates = 0
        for event in events:
            event_id = event["event_id"]
            if event_id in in_batch or self._seen.get(event_id) == fingerprint(event):
                duplicates += 1
                continue
            in_batch.add(event_id)
            fresh.append(event)
        return fresh, duplicates

    def commit(self, events: Iterable[dict]) -> None:
        for event in events:
            self._seen.pop(event["event_id"], None)  # re-insert so revised items move to the end
            self._seen[event["event_id"]] = fingerprint(event)
        while len(self._seen) > self._max:
            self._seen.pop(next(iter(self._seen)))

    def save(self) -> None:
        if not self._path:
            return
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        tmp = f"{self._path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(self._seen, handle)
        os.replace(tmp, self._path)

    def _load(self) -> None:
        try:
            with open(self._path, encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                self._seen = {str(k): str(v) for k, v in data.items()}
        except FileNotFoundError:
            pass
        except (OSError, ValueError):
            self._seen = {}  # corrupt state: safe to rebuild, Mongo upserts are idempotent
