"""Connectivity check for the whole pipeline. Run from the project root:

    python scripts/check_connectivity.py            # API + Kafka + MongoDB
    python scripts/check_connectivity.py --skip-api # do not touch the GarbinWx API

Exit code 0 only if every check that was run passed. Makes at most ONE request to the API.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from eqmon.config import load_settings  # noqa: E402


def check_api(s) -> bool:
    import requests

    from eqmon.normalize import normalize_feed

    try:
        r = requests.get(s.api_url, headers={"User-Agent": s.user_agent}, timeout=s.request_timeout_s)
        r.raise_for_status()
        feed = normalize_feed(r.json())
    except Exception as exc:  # noqa: BLE001 - report any failure
        print(f"[FAIL] GarbinWx API: {exc}")
        return False
    print(f"[ OK ] GarbinWx API: HTTP {r.status_code}, {feed.feed_total} records "
          f"({len(feed.events)} valid, {len(feed.rejected)} rejected), last_updated={feed.source_last_updated}")
    return True


def check_kafka(s) -> bool:
    try:
        from confluent_kafka.admin import AdminClient

        meta = AdminClient({"bootstrap.servers": s.kafka_bootstrap}).list_topics(timeout=10)
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] Kafka at {s.kafka_bootstrap}: {exc}")
        return False
    if s.kafka_topic in meta.topics and meta.topics[s.kafka_topic].error is None:
        print(f"[ OK ] Kafka at {s.kafka_bootstrap}: topic '{s.kafka_topic}' exists")
        return True
    print(f"[FAIL] Kafka reachable but topic '{s.kafka_topic}' is missing (docker compose up -d creates it)")
    return False


def check_mongo(s) -> bool:
    from eqmon.db import count_events, ensure_indexes, fetch_status, get_client

    try:
        client = get_client(s.mongo_uri, timeout_ms=4000)
        client.admin.command("ping")
        db = client[s.mongo_db]
        ensure_indexes(db)
        status = fetch_status(db)
        n = count_events(db)
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] MongoDB at {s.mongo_uri}: {exc}")
        return False
    print(f"[ OK ] MongoDB: {n} events in '{s.mongo_db}.events', indexes ensured")
    print(f"       producer heartbeat: {status['producer'] and status['producer'].get('fetched_at')}")
    print(f"       spark last batch:   {status['spark'] and status['spark'].get('last_batch_at')}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-api", action="store_true")
    args = parser.parse_args()
    s = load_settings()
    results = [] if args.skip_api else [check_api(s)]
    results += [check_kafka(s), check_mongo(s)]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
