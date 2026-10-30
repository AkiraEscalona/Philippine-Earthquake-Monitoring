"""Decide whether the pipeline is delivering fresh data, from the status docs in MongoDB."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

DEFAULT_POLL_S = 300
STALE_UPSTREAM = timedelta(hours=3)


@dataclass
class Freshness:
    level: str  # "ok" | "delayed" | "stale" | "unknown"
    headline: str
    notes: list[str] = field(default_factory=list)


def humanize(delta: timedelta) -> str:
    seconds = max(int(delta.total_seconds()), 0)
    if seconds < 90:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 120:
        return f"{minutes} min"
    hours = minutes // 60
    return f"{hours} h" if hours < 48 else f"{hours // 24} d"


def assess(producer: dict | None, spark: dict | None, now: datetime | None = None) -> Freshness:
    """``producer``/``spark`` are the documents from the pipeline_status collection."""
    now = now or datetime.now(timezone.utc)
    if not producer or not producer.get("fetched_at"):
        return Freshness(
            "unknown",
            "No producer heartbeat found in MongoDB. Start the producer and the Spark job.",
        )

    poll = int(producer.get("poll_interval_s") or DEFAULT_POLL_S)
    age = now - producer["fetched_at"]
    notes: list[str] = []

    updated = producer.get("source_last_updated")
    if updated and now - updated > STALE_UPSTREAM:
        notes.append(f"Upstream feed itself was last updated {humanize(now - updated)} ago.")
    if spark and spark.get("last_batch_at"):
        notes.append(f"Spark last wrote a batch {humanize(now - spark['last_batch_at'])} ago.")
    else:
        notes.append("Spark has not reported a batch yet.")

    ok_limit = timedelta(seconds=2 * poll + 120)
    if age <= ok_limit:
        return Freshness("ok", f"Live - last successful API poll {humanize(age)} ago.", notes)
    if age <= 6 * ok_limit:
        return Freshness("delayed", f"Delayed - last API poll was {humanize(age)} ago.", notes)
    return Freshness("stale", f"Stale - no API poll for {humanize(age)}. Data shown may be outdated.", notes)
