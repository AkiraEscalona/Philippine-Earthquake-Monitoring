"""Central configuration, read from environment variables (optionally a .env file)."""
from __future__ import annotations

import os
from dataclasses import dataclass

try:  # python-dotenv is optional at import time
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

# Hard floor so a typo in .env can never hammer the upstream API.
MIN_POLL_INTERVAL_S = 60

# MongoDB collection names (shared by the Spark job and the dashboard).
EVENTS_COLLECTION = "events"
REJECTED_COLLECTION = "rejected_events"
STATUS_COLLECTION = "pipeline_status"

ATTRIBUTION = (
    "Earthquake data: PHIVOLCS, redistributed by the GarbinWx Public API "
    "(https://garbinwx.org/developer) under CC BY-SA 4.0. "
    "Not an official warning source."
)


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    api_url: str
    user_agent: str
    poll_interval_s: int
    request_timeout_s: int
    max_backoff_s: int
    kafka_bootstrap: str
    kafka_topic: str
    mongo_uri: str
    mongo_db: str
    state_path: str
    dashboard_refresh_s: int


def load_settings() -> Settings:
    requested = _int("POLL_INTERVAL_SECONDS", 300)
    return Settings(
        api_url=os.getenv("GARBINWX_URL", "https://data.garbinwx.org/api/earthquakes.json"),
        user_agent=os.getenv(
            "USER_AGENT", "PhilippineEarthquakeMonitoring/1.0 (student project; set USER_AGENT in .env)"
        ),
        poll_interval_s=max(requested, MIN_POLL_INTERVAL_S),
        request_timeout_s=_int("REQUEST_TIMEOUT_SECONDS", 20),
        max_backoff_s=_int("MAX_BACKOFF_SECONDS", 900),
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "philippine-earthquakes"),
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.getenv("MONGO_DB", "earthquake_monitor"),
        state_path=os.getenv("PRODUCER_STATE_PATH", ".state/producer_seen.json"),
        dashboard_refresh_s=_int("DASHBOARD_REFRESH_SECONDS", 15),
    )
