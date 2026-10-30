import unittest
from datetime import datetime, timezone

from eqmon.normalize import (
    FeedFormatError, make_event_id, normalize_feed, normalize_record, parse_timestamp,
)

NOW = datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)

GOOD = {
    "timestamp_iso": "2026-09-05T10:32:00+08:00",
    "latitude": 16.04, "longitude": 120.04, "depth_km": 34, "magnitude": 1.5,
    "location": "006 km S 63° W of Sual (Pangasinan)",
    "bulletin_url": "https://earthquake.phivolcs.dost.gov.ph/2026_Earthquake_Information\\September\\2026_0905_0232_B1.html",
}


class NormalizeRecordTests(unittest.TestCase):
    def test_valid_record_is_normalized_to_utc(self):
        event, reason = normalize_record(GOOD, now=NOW)
        self.assertIsNone(reason)
        self.assertEqual(event["timestamp_utc"], "2026-09-05T02:32:00Z")
        self.assertEqual(event["magnitude"], 1.5)
        self.assertEqual(event["depth_km"], 34.0)
        self.assertEqual(event["kind"], "event")

    def test_backslashes_in_bulletin_url_are_fixed(self):
        event, _ = normalize_record(GOOD, now=NOW)
        self.assertNotIn("\\", event["bulletin_url"])
        self.assertTrue(event["bulletin_url"].endswith("/September/2026_0905_0232_B1.html"))

    def test_naive_timestamp_is_assumed_philippine_time(self):
        self.assertEqual(parse_timestamp("2026-09-05T10:32:00").hour, 2)

    def test_z_suffix_is_accepted(self):
        self.assertEqual(parse_timestamp("2026-09-05T02:32:00Z").hour, 2)

    def test_event_id_ignores_magnitude_and_depth_revisions(self):
        a, _ = normalize_record(GOOD, now=NOW)
        b, _ = normalize_record({**GOOD, "magnitude": 1.9, "depth_km": 10}, now=NOW)
        self.assertEqual(a["event_id"], b["event_id"])

    def test_event_id_differs_for_different_epicentre(self):
        t = parse_timestamp(GOOD["timestamp_iso"])
        self.assertNotEqual(make_event_id(t, 16.04, 120.04), make_event_id(t, 16.05, 120.04))

    def test_rejections(self):
        cases = {
            "bad_timestamp": {**GOOD, "timestamp_iso": "yesterday"},
            "timestamp_in_future": {**GOOD, "timestamp_iso": "2027-01-01T00:00:00+08:00"},
            "bad_latitude": {**GOOD, "latitude": 91},
            "bad_longitude": {**GOOD, "longitude": "abc"},
            "bad_magnitude": {**GOOD, "magnitude": None},
            "bad_depth": {**GOOD, "depth_km": -5},
            "not_an_object": "oops",
        }
        for expected, raw in cases.items():
            with self.subTest(expected):
                event, reason = normalize_record(raw, now=NOW)
                self.assertIsNone(event)
                self.assertEqual(reason, expected)

    def test_booleans_and_nan_are_not_numbers(self):
        self.assertEqual(normalize_record({**GOOD, "magnitude": True}, now=NOW)[1], "bad_magnitude")
        self.assertEqual(normalize_record({**GOOD, "magnitude": float("nan")}, now=NOW)[1], "bad_magnitude")

    def test_missing_depth_and_location_are_tolerated(self):
        raw = {k: v for k, v in GOOD.items() if k not in ("depth_km", "location")}
        event, reason = normalize_record(raw, now=NOW)
        self.assertIsNone(reason)
        self.assertIsNone(event["depth_km"])
        self.assertEqual(event["location"], "Unknown location")


class NormalizeFeedTests(unittest.TestCase):
    def test_feed_splits_valid_and_rejected(self):
        payload = {
            "metadata": {"last_updated": "2026-10-30T06:08:25.663659+08:00", "total_earthquakes": 3},
            "earthquakes": [GOOD, {**GOOD, "magnitude": "x"}, {**GOOD, "latitude": 14.0}],
        }
        feed = normalize_feed(payload, now=NOW)
        self.assertEqual(len(feed.events), 2)
        self.assertEqual(feed.rejection_summary(), {"bad_magnitude": 1})
        self.assertEqual(feed.source_last_updated, "2026-10-29T22:08:25Z")
        self.assertEqual(feed.feed_total, 3)
        self.assertEqual(feed.declared_total, 3)
        self.assertTrue(all(e["source_last_updated"] == feed.source_last_updated for e in feed.events))

    def test_bad_payload_shapes_raise(self):
        for payload in (None, [], {"earthquakes": "nope"}, {"metadata": {}}):
            with self.subTest(payload=payload), self.assertRaises(FeedFormatError):
                normalize_feed(payload)

    def test_empty_feed_is_valid_but_empty(self):
        feed = normalize_feed({"earthquakes": []})
        self.assertEqual((feed.events, feed.feed_total), ([], 0))


if __name__ == "__main__":
    unittest.main()
