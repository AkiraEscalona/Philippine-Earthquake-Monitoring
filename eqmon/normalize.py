"""Validation and normalization of GarbinWx earthquake records (pure functions, no I/O)."""
from __future__ import annotations

import hashlib
import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

SOURCE_NAME = "GarbinWx/PHIVOLCS"
# The Philippines has no DST, so a fixed offset avoids needing tzdata in the producer.
PH_TZ = timezone(timedelta(hours=8))

MAG_MIN, MAG_MAX = -2.0, 10.0
DEPTH_MIN, DEPTH_MAX = 0.0, 800.0
FUTURE_TOLERANCE = timedelta(minutes=10)


class FeedFormatError(ValueError):
    """The API response does not look like the documented GarbinWx earthquake feed."""


@dataclass
class FeedResult:
    events: list[dict] = field(default_factory=list)
    rejected: list[tuple[str, Any]] = field(default_factory=list)
    source_last_updated: str | None = None
    feed_total: int = 0
    declared_total: int | None = None

    def rejection_summary(self) -> dict[str, int]:
        return dict(Counter(reason for reason, _ in self.rejected))


def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def parse_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 string into an aware UTC datetime. Naive values are assumed PH time."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text[-1] in "Zz":
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=PH_TZ)
    return parsed.astimezone(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_event_id(time_utc: datetime, latitude: float, longitude: float) -> str:
    """Stable identity of an earthquake: origin time + epicentre.

    Magnitude/depth are deliberately excluded so that PHIVOLCS revisions update the
    same MongoDB document instead of creating a duplicate.
    """
    key = f"{iso_utc(time_utc)}|{latitude:.3f}|{longitude:.3f}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:24]


def _clean_url(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    url = value.strip().replace("\\", "/")  # the feed ships Windows-style separators
    return url if url.lower().startswith(("http://", "https://")) else None


def normalize_record(raw: Any, *, now: datetime | None = None) -> tuple[dict | None, str | None]:
    """Return (event, None) if valid, otherwise (None, reason)."""
    if not isinstance(raw, dict):
        return None, "not_an_object"
    now = now or datetime.now(timezone.utc)

    time_utc = parse_timestamp(raw.get("timestamp_iso"))
    if time_utc is None:
        return None, "bad_timestamp"
    if time_utc > now + FUTURE_TOLERANCE:
        return None, "timestamp_in_future"

    lat, lon = _num(raw.get("latitude")), _num(raw.get("longitude"))
    if lat is None or not -90 <= lat <= 90:
        return None, "bad_latitude"
    if lon is None or not -180 <= lon <= 180:
        return None, "bad_longitude"

    mag = _num(raw.get("magnitude"))
    if mag is None or not MAG_MIN <= mag <= MAG_MAX:
        return None, "bad_magnitude"

    depth: float | None = None
    if raw.get("depth_km") is not None:
        depth = _num(raw.get("depth_km"))
        if depth is None or not DEPTH_MIN <= depth <= DEPTH_MAX:
            return None, "bad_depth"

    location = raw.get("location")
    location = " ".join(location.split()) if isinstance(location, str) else ""

    return {
        "kind": "event",
        "event_id": make_event_id(time_utc, lat, lon),
        "timestamp_utc": iso_utc(time_utc),
        "latitude": lat,
        "longitude": lon,
        "depth_km": depth,
        "magnitude": mag,
        "location": location or "Unknown location",
        "bulletin_url": _clean_url(raw.get("bulletin_url")),
        "source": SOURCE_NAME,
    }, None


def normalize_feed(
    payload: Any, *, fetched_at: datetime | None = None, now: datetime | None = None
) -> FeedResult:
    """Validate the whole API payload and normalize every record in it."""
    if not isinstance(payload, dict) or not isinstance(payload.get("earthquakes"), list):
        raise FeedFormatError("Response is not a JSON object with an 'earthquakes' list")

    fetched_at = fetched_at or datetime.now(timezone.utc)
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    updated = parse_timestamp(metadata.get("last_updated"))
    declared = metadata.get("total_earthquakes")

    result = FeedResult(
        source_last_updated=iso_utc(updated) if updated else None,
        feed_total=len(payload["earthquakes"]),
        declared_total=declared if isinstance(declared, int) else None,
    )
    for raw in payload["earthquakes"]:
        event, reason = normalize_record(raw, now=now)
        if event is None:
            result.rejected.append((reason or "invalid", raw))
            continue
        event["fetched_at"] = iso_utc(fetched_at)
        event["source_last_updated"] = result.source_last_updated
        result.events.append(event)
    return result
