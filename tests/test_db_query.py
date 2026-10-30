import unittest
from datetime import datetime, timezone
from importlib.util import find_spec


@unittest.skipUnless(find_spec("pymongo"), "pymongo not installed")
class BuildQueryTests(unittest.TestCase):
    def test_empty_filters_give_empty_query(self):
        from eqmon.db import build_query
        self.assertEqual(build_query(), {})

    def test_all_filters(self):
        from eqmon.db import build_query
        start, end = datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 2, 1, tzinfo=timezone.utc)
        q = build_query(start, end, 2.5, 6.0, "Sual (Pang", ["Cebu"])
        self.assertEqual(q["event_time"], {"$gte": start, "$lt": end})
        self.assertEqual(q["magnitude"], {"$gte": 2.5, "$lte": 6.0})
        self.assertEqual(q["region"], {"$in": ["Cebu"]})
        self.assertEqual(q["location"]["$regex"], r"Sual\ \(Pang")  # user text is escaped
        self.assertEqual(q["location"]["$options"], "i")

    def test_zero_magnitude_is_a_real_filter(self):
        from eqmon.db import build_query
        self.assertEqual(build_query(min_mag=0.0), {"magnitude": {"$gte": 0.0}})


if __name__ == "__main__":
    unittest.main()
