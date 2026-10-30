"""MongoDB access for the dashboard and scripts (read side of the pipeline)."""
from __future__ import annotations

import re
from datetime import datetime, timezone

import pandas as pd
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.database import Database

from .config import EVENTS_COLLECTION, REJECTED_COLLECTION, STATUS_COLLECTION

EVENT_COLUMNS = [
    "event_id", "event_time", "latitude", "longitude", "depth_km", "magnitude",
    "magnitude_class", "location", "region", "bulletin_url", "ingested_at",
]


def get_client(uri: str, timeout_ms: int = 3000) -> MongoClient:
    return MongoClient(uri, serverSelectionTimeoutMS=timeout_ms, tz_aware=True, tzinfo=timezone.utc)


def ensure_indexes(db: Database) -> None:
    """Idempotent. _id (= event_id) already enforces uniqueness / deduplication."""
    events = db[EVENTS_COLLECTION]
    events.create_index([("event_time", DESCENDING)], name="event_time_desc")
    events.create_index([("magnitude", DESCENDING)], name="magnitude_desc")
    events.create_index([("region", ASCENDING), ("event_time", DESCENDING)], name="region_time")
    events.create_index([("ingested_at", DESCENDING)], name="ingested_at_desc")
    db[REJECTED_COLLECTION].create_index([("rejected_at", DESCENDING)], name="rejected_at_desc")


def build_query(
    start: datetime | None = None,
    end: datetime | None = None,
    min_mag: float | None = None,
    max_mag: float | None = None,
    location_text: str | None = None,
    regions: list[str] | None = None,
) -> dict:
    """Translate UI filters into a MongoDB filter. ``start`` inclusive, ``end`` exclusive."""
    query: dict = {}
    if start or end:
        query["event_time"] = {}
        if start:
            query["event_time"]["$gte"] = start
        if end:
            query["event_time"]["$lt"] = end
    if min_mag is not None or max_mag is not None:
        query["magnitude"] = {}
        if min_mag is not None:
            query["magnitude"]["$gte"] = min_mag
        if max_mag is not None:
            query["magnitude"]["$lte"] = max_mag
    if location_text and location_text.strip():
        query["location"] = {"$regex": re.escape(location_text.strip()), "$options": "i"}
    if regions:
        query["region"] = {"$in": list(regions)}
    return query


def fetch_events(db: Database, query: dict, limit: int = 50_000) -> pd.DataFrame:
    projection = {c: 1 for c in EVENT_COLUMNS if c != "event_id"}
    docs = list(db[EVENTS_COLLECTION].find(query, projection).sort("event_time", DESCENDING).limit(limit))
    if not docs:
        return pd.DataFrame(columns=EVENT_COLUMNS)
    df = pd.DataFrame(docs).rename(columns={"_id": "event_id"})
    for column in EVENT_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    return df[EVENT_COLUMNS]


def fetch_status(db: Database) -> dict:
    docs = {d["_id"]: d for d in db[STATUS_COLLECTION].find({})}
    return {"producer": docs.get("producer"), "spark": docs.get("spark")}


def distinct_regions(db: Database) -> list[str]:
    return sorted(r for r in db[EVENTS_COLLECTION].distinct("region") if r)


def count_events(db: Database) -> int:
    return db[EVENTS_COLLECTION].estimated_document_count()
