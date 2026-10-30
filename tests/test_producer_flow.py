import json
import random
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import producer
from eqmon.api_client import (
    FetchError, FetchResult, GarbinWxClient, compute_backoff, parse_max_age, parse_retry_after,
)
from eqmon.config import MIN_POLL_INTERVAL_S, Settings
from eqmon.dedupe import Deduper
from eqmon.freshness import assess

SETTINGS = Settings("u", "ua", 300, 5, 900, "k", "t", "m", "d", "", 15)


def quake(minute, mag=2.0):
    ts = datetime(2026, 10, 20, 1, minute, tzinfo=timezone.utc).isoformat()
    return {"timestamp_iso": ts, "latitude": 10.0, "longitude": 120.0, "depth_km": 5, "magnitude": mag,
            "location": "001 km N of X (Cebu)", "bulletin_url": "https://example.org/a.html"}


class FakeClient:
    def __init__(self, results):
        self.results = list(results)

    def fetch(self):
        item = self.results.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakePublisher:
    def __init__(self, fail=()):
        self.sent, self.fail = [], set(fail)

    def publish(self, messages):
        messages = list(messages)
        self.sent.extend(messages)
        return {k for k, _ in messages if k in self.fail}

    def close(self):
        pass


def payload(*quakes):
    return {"metadata": {"last_updated": "2026-10-30T09:00:00+08:00", "total_earthquakes": len(quakes)},
            "earthquakes": list(quakes)}


class PollOnceTests(unittest.TestCase):
    def test_publishes_new_events_plus_heartbeat_and_skips_repeats(self):
        pub, dd = FakePublisher(), Deduper()
        client = FakeClient([FetchResult(200, payload(quake(1), quake(2))), FetchResult(200, payload(quake(1), quake(2), quake(3)))])
        s1 = producer.poll_once(client, pub, dd, SETTINGS)
        s2 = producer.poll_once(client, pub, dd, SETTINGS)
        self.assertEqual((s1["published"], s1["duplicates"]), (2, 0))
        self.assertEqual((s2["published"], s2["duplicates"]), (1, 2))
        kinds = [m["kind"] for _, m in pub.sent]
        self.assertEqual(kinds.count("event"), 3)
        self.assertEqual(kinds.count("heartbeat"), 2)
        heartbeat = [m for _, m in pub.sent if m["kind"] == "heartbeat"][-1]
        self.assertEqual((heartbeat["feed_total"], heartbeat["published"], heartbeat["poll_interval_s"]), (3, 1, 300))

    def test_undelivered_events_are_retried_next_poll(self):
        dd = Deduper()
        first_id = None
        pub = FakePublisher()
        client = FakeClient([FetchResult(200, payload(quake(1))), FetchResult(200, payload(quake(1)))])
        first_id = None
        # make the first delivery fail
        orig = pub.publish

        def failing_once(messages):
            nonlocal first_id
            messages = list(messages)
            ids = [k for k, m in messages if m["kind"] == "event"]
            if ids and first_id is None:
                first_id = ids[0]
                pub.fail = {first_id}
            else:
                pub.fail = set()
            return orig(messages)

        pub.publish = failing_once
        with self.assertRaises(ConnectionError):
            producer.poll_once(client, pub, dd, SETTINGS)
        s2 = producer.poll_once(client, pub, dd, SETTINGS)
        self.assertEqual(s2["published"], 1)

    def test_invalid_records_are_counted_not_published(self):
        pub = FakePublisher()
        client = FakeClient([FetchResult(200, payload(quake(1), {**quake(2), "magnitude": "bad"}))])
        s = producer.poll_once(client, pub, Deduper(), SETTINGS)
        self.assertEqual((s["valid"], s["rejected"], s["published"]), (1, 1, 1))

    def test_304_sends_only_a_heartbeat(self):
        pub = FakePublisher()
        s = producer.poll_once(FakeClient([FetchResult(304)]), pub, Deduper(), SETTINGS, "2026-10-30T01:00:00Z")
        self.assertEqual([m["kind"] for _, m in pub.sent], ["heartbeat"])
        self.assertEqual(s["status"], 304)

    def test_dry_run_never_needs_a_publisher(self):
        s = producer.poll_once(FakeClient([FetchResult(200, payload(quake(1)))]), None, Deduper(), SETTINGS)
        self.assertEqual(s["valid"], 1)


class RunLoopTests(unittest.TestCase):
    def test_once_mode_returns_nonzero_on_fetch_error(self):
        stop = mock.Mock(is_set=mock.Mock(return_value=False), wait=mock.Mock())
        with mock.patch.object(producer, "GarbinWxClient", return_value=FakeClient([FetchError("HTTP 500")])):
            self.assertEqual(producer.run(SETTINGS, FakePublisher(), stop, once=True), 1)

    def test_backoff_wait_honours_retry_after_and_stop_event_ends_loop(self):
        import threading
        stop = threading.Event()
        waits = []
        stop.wait = lambda t: (waits.append(t), stop.set())  # type: ignore[assignment]
        with mock.patch.object(producer, "GarbinWxClient", return_value=FakeClient([FetchError("HTTP 429", retry_after=700)])):
            producer.run(SETTINGS, FakePublisher(), stop)
        self.assertGreaterEqual(waits[0], 700)


class ClientHelperTests(unittest.TestCase):
    def test_backoff_grows_and_is_capped(self):
        rng = random.Random(1)
        delays = [compute_backoff(n, base=30, cap=900, rng=rng) for n in range(1, 10)]
        self.assertLess(delays[0], delays[3])
        self.assertTrue(all(d <= 900 for d in delays))
        self.assertGreaterEqual(delays[0], 24)

    def test_header_parsing(self):
        self.assertEqual(parse_retry_after("120"), 120.0)
        self.assertIsNone(parse_retry_after("Wed, 21 Oct 2026 07:28:00 GMT"))
        self.assertEqual(parse_max_age("public, max-age=600"), 600)
        self.assertIsNone(parse_max_age(None))

    def test_conditional_headers_and_status_handling(self):
        def resp(status, body=None, headers=None):
            r = mock.Mock(status_code=status, headers=headers or {})
            r.json.return_value = body
            return r

        session = mock.MagicMock()
        session.headers = {}
        session.get.side_effect = [
            resp(200, {"earthquakes": []}, {"ETag": "abc"}),
            resp(304),
            resp(429, headers={"Retry-After": "90"}),
            resp(500),
        ]
        client = GarbinWxClient("http://x", "ua", session=session)
        self.assertEqual(client.fetch().status, 200)
        self.assertEqual(client.fetch().status, 304)
        self.assertEqual(session.get.call_args_list[1].kwargs["headers"], {"If-None-Match": "abc"})
        with self.assertRaises(FetchError) as ctx:
            client.fetch()
        self.assertEqual(ctx.exception.retry_after, 90.0)
        with self.assertRaises(FetchError):
            client.fetch()

    def test_poll_interval_is_clamped(self):
        import os
        with mock.patch.dict(os.environ, {"POLL_INTERVAL_SECONDS": "15"}):
            from eqmon.config import load_settings
            self.assertEqual(load_settings().poll_interval_s, MIN_POLL_INTERVAL_S)


class FreshnessTests(unittest.TestCase):
    NOW = datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)

    def prod(self, age_s, poll=300):
        return {"fetched_at": self.NOW - timedelta(seconds=age_s), "poll_interval_s": poll,
                "source_last_updated": self.NOW - timedelta(hours=5)}

    def test_levels(self):
        spark = {"last_batch_at": self.NOW - timedelta(seconds=60)}
        self.assertEqual(assess(self.prod(200), spark, self.NOW).level, "ok")
        self.assertEqual(assess(self.prod(1500), spark, self.NOW).level, "delayed")
        self.assertEqual(assess(self.prod(20000), spark, self.NOW).level, "stale")
        self.assertEqual(assess(None, None, self.NOW).level, "unknown")

    def test_upstream_staleness_is_reported_separately(self):
        f = assess(self.prod(100), None, self.NOW)
        self.assertEqual(f.level, "ok")
        self.assertTrue(any("Upstream" in n for n in f.notes))


if __name__ == "__main__":
    unittest.main()
