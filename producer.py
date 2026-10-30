"""GarbinWx -> Kafka producer.

Polls the GarbinWx earthquake feed at a conservative interval (default 5 min, never below
60 s), validates and normalizes records, publishes only new/revised earthquakes to Kafka,
and sends one heartbeat message per poll so the dashboard can show data freshness.

Usage (PowerShell):
    python producer.py              # run forever
    python producer.py --once       # one poll, then exit
    python producer.py --dry-run    # fetch + validate, print a summary, do NOT touch Kafka
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
from datetime import datetime, timezone

from eqmon.api_client import FetchError, GarbinWxClient, compute_backoff
from eqmon.config import Settings, load_settings
from eqmon.dedupe import Deduper
from eqmon.kafka_io import Publisher
from eqmon.normalize import FeedFormatError, iso_utc, normalize_feed

log = logging.getLogger("producer")


def build_heartbeat(
    fetched_at: datetime, source_last_updated: str | None, feed_total: int, valid: int,
    rejected: int, published: int, poll_interval_s: int,
) -> dict:
    return {
        "kind": "heartbeat",
        "fetched_at": iso_utc(fetched_at),
        "source_last_updated": source_last_updated,
        "feed_total": feed_total,
        "valid": valid,
        "rejected": rejected,
        "published": published,
        "poll_interval_s": poll_interval_s,
    }


def poll_once(
    client: GarbinWxClient, publisher: Publisher | None, deduper: Deduper, settings: Settings,
    last_source_updated: str | None = None,
) -> dict:
    """One fetch -> normalize -> dedupe -> publish cycle. Raises FetchError / FeedFormatError."""
    fetched_at = datetime.now(timezone.utc)
    result = client.fetch()

    if result.status == 304:  # unchanged upstream
        summary = {"status": 304, "feed_total": 0, "valid": 0, "rejected": 0, "published": 0, "duplicates": 0,
                   "source_last_updated": last_source_updated, "max_age": result.max_age}
        if publisher:
            publisher.publish([("heartbeat", build_heartbeat(fetched_at, last_source_updated, 0, 0, 0, 0, settings.poll_interval_s))])
        return summary

    feed = normalize_feed(result.payload, fetched_at=fetched_at)
    if feed.declared_total is not None and feed.declared_total != feed.feed_total:
        log.warning("metadata.total_earthquakes=%s but feed contains %s records", feed.declared_total, feed.feed_total)
    if feed.rejected:
        log.warning("Rejected %d records: %s", len(feed.rejected), feed.rejection_summary())
    if not feed.events:
        log.warning("Feed contained no valid earthquakes")

    fresh, duplicates = deduper.filter_new(feed.events)
    failed: set[str] = set()
    if publisher:
        messages = [(e["event_id"], e) for e in fresh]
        failed = publisher.publish(messages) if messages else set()
        published_events = [e for e in fresh if e["event_id"] not in failed]
        deduper.commit(published_events)  # only remember what Kafka acknowledged
        deduper.save()
        publisher.publish([("heartbeat", build_heartbeat(
            fetched_at, feed.source_last_updated, feed.feed_total, len(feed.events),
            len(feed.rejected), len(published_events), settings.poll_interval_s))])
        if failed:
            raise ConnectionError(f"{len(failed)} messages were not delivered to Kafka; they will be retried")
    return {
        "status": 200, "feed_total": feed.feed_total, "valid": len(feed.events), "rejected": len(feed.rejected),
        "published": len(fresh) - len(failed), "duplicates": duplicates,
        "source_last_updated": feed.source_last_updated, "max_age": result.max_age,
    }


def run(settings: Settings, publisher: Publisher | None, stop: threading.Event, once: bool = False) -> int:
    client = GarbinWxClient(settings.api_url, settings.user_agent, settings.request_timeout_s)
    deduper = Deduper(settings.state_path)
    failures = 0
    last_source_updated: str | None = None
    exit_code = 0
    log.info("Source: %s | poll every %ss | topic=%s", settings.api_url, settings.poll_interval_s, settings.kafka_topic)

    while not stop.is_set():
        wait = settings.poll_interval_s
        try:
            summary = poll_once(client, publisher, deduper, settings, last_source_updated)
            last_source_updated = summary["source_last_updated"] or last_source_updated
            failures = 0
            log.info("Poll OK: %s", json.dumps(summary))
            if summary.get("max_age"):  # honour the server's own cache guidance if it is longer
                wait = max(wait, summary["max_age"])
        except (FetchError, FeedFormatError, ConnectionError) as exc:
            failures += 1
            exit_code = 1
            wait = compute_backoff(failures, cap=settings.max_backoff_s)
            if isinstance(exc, FetchError) and exc.retry_after:
                wait = max(wait, exc.retry_after)
            log.error("Poll failed (%d in a row): %s - retrying in %.0fs", failures, exc, wait)
        else:
            exit_code = 0
        if once:
            break
        stop.wait(wait)  # interruptible sleep -> prompt Ctrl+C shutdown

    if publisher:
        publisher.close()
    log.info("Producer stopped.")
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--once", action="store_true", help="poll one time and exit")
    parser.add_argument("--dry-run", action="store_true", help="fetch and validate only; no Kafka")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    settings = load_settings()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: (log.info("Shutdown requested..."), stop.set()))

    publisher: Publisher | None = None
    if not args.dry_run:
        from eqmon.kafka_io import KafkaPublisher

        try:
            publisher = KafkaPublisher(settings.kafka_bootstrap, settings.kafka_topic)
            publisher.wait_until_ready()
        except ImportError:
            log.error("confluent-kafka is not installed. Run: pip install -r requirements.txt")
            return 2
        except ConnectionError as exc:
            log.error("%s. Is Docker running? Try: docker compose up -d", exc)
            return 2
    return run(settings, publisher, stop, once=args.once or args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
