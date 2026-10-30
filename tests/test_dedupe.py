import os
import tempfile
import unittest

from eqmon.dedupe import Deduper


def ev(event_id="a", magnitude=2.0, **extra):
    return {"event_id": event_id, "magnitude": magnitude, "depth_km": 10.0, "latitude": 1.0,
            "longitude": 2.0, "location": "x", "bulletin_url": None, **extra}


class DeduperTests(unittest.TestCase):
    def test_new_events_pass_and_repeats_are_dropped_after_commit(self):
        d = Deduper()
        fresh, dups = d.filter_new([ev("a"), ev("b")])
        self.assertEqual((len(fresh), dups), (2, 0))
        d.commit(fresh)
        fresh, dups = d.filter_new([ev("a"), ev("b")])
        self.assertEqual((len(fresh), dups), (0, 2))

    def test_uncommitted_events_are_retried(self):
        d = Deduper()
        d.filter_new([ev("a")])  # never committed (e.g. Kafka delivery failed)
        fresh, _ = d.filter_new([ev("a")])
        self.assertEqual(len(fresh), 1)

    def test_revised_magnitude_is_republished(self):
        d = Deduper()
        d.commit(d.filter_new([ev("a", 2.0)])[0])
        fresh, dups = d.filter_new([ev("a", 2.4)])
        self.assertEqual((len(fresh), dups), (1, 0))

    def test_ingest_only_metadata_changes_do_not_republish(self):
        d = Deduper()
        d.commit(d.filter_new([ev("a", fetched_at="t1")])[0])
        fresh, _ = d.filter_new([ev("a", fetched_at="t2")])
        self.assertEqual(fresh, [])

    def test_duplicates_inside_one_feed_collapse(self):
        fresh, dups = Deduper().filter_new([ev("a"), ev("a"), ev("b")])
        self.assertEqual(([e["event_id"] for e in fresh], dups), (["a", "b"], 1))

    def test_state_persists_to_disk_and_survives_corruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "sub", "seen.json")
            d = Deduper(path)
            d.commit([ev("a")])
            d.save()
            self.assertEqual(Deduper(path).filter_new([ev("a")])[0], [])
            with open(path, "w") as fh:
                fh.write("{not json")
            self.assertEqual(len(Deduper(path)), 0)

    def test_max_entries_evicts_oldest(self):
        d = Deduper(max_entries=2)
        d.commit([ev("a"), ev("b"), ev("c")])
        self.assertEqual(len(d), 2)
        self.assertEqual(len(d.filter_new([ev("a")])[0]), 1)  # "a" was evicted


if __name__ == "__main__":
    unittest.main()
