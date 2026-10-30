import io
import unittest

import pandas as pd

from eqmon.aggregate import (
    MAG_LABELS, counts_by_day, counts_by_hour, extract_region, magnitude_class,
    magnitude_distribution, magnitude_stats, region_stats,
)
from eqmon.export import to_csv_bytes, to_excel_bytes


def frame():
    rows = [
        # (UTC time, magnitude, depth, location)
        ("2026-09-05T02:32:00Z", 1.5, 34, "006 km S 63° W of Sual (Pangasinan)"),
        ("2026-09-05T20:00:00Z", 4.0, 10, "010 km N of Abra (Abra)"),          # 04:00 PH on Sep 6
        ("2026-09-07T01:00:00Z", 6.2, 50, "050 km E of Bislig (Surigao del Sur)"),
        ("2026-09-07T02:00:00Z", 2.0, 20, "003 km W of Sual (Pangasinan)"),
    ]
    df = pd.DataFrame(rows, columns=["event_time", "magnitude", "depth_km", "location"])
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["latitude"], df["longitude"], df["event_id"] = 10.0, 120.0, ["a", "b", "c", "d"]
    return df


class AggregateTests(unittest.TestCase):
    def test_extract_region_uses_last_parenthesis(self):
        self.assertEqual(extract_region("014 km S of Balut Island (Municipality Of Sarangani) (Davao Occidental)"), "Davao Occidental")
        self.assertEqual(extract_region("006 km S 63° W of Sual (Pangasinan)"), "Pangasinan")
        self.assertEqual(extract_region("Somewhere offshore"), "Unknown")
        self.assertEqual(extract_region(None), "Unknown")

    def test_magnitude_class_boundaries(self):
        self.assertEqual([magnitude_class(m) for m in (0.0, 1.99, 2.0, 3.5, 4.0, 5.9, 6.0, 8.1)],
                         ["<2", "<2", "2-3", "3-4", "4-5", "5-6", ">=6", ">=6"])

    def test_magnitude_stats(self):
        s = magnitude_stats(frame())
        self.assertEqual(s["count"], 4)
        self.assertAlmostEqual(s["mean"], (1.5 + 4 + 6.2 + 2) / 4)
        self.assertEqual((s["min"], s["max"]), (1.5, 6.2))
        self.assertEqual(magnitude_stats(frame().iloc[0:0])["count"], 0)
        self.assertIsNone(magnitude_stats(frame().iloc[0:0])["mean"])

    def test_magnitude_distribution_includes_empty_ranges(self):
        d = magnitude_distribution(frame())
        self.assertEqual(list(d["magnitude_range"]), MAG_LABELS)
        self.assertEqual(list(d["count"]), [1, 1, 0, 1, 0, 1])
        self.assertEqual(list(magnitude_distribution(frame().iloc[0:0])["count"]), [0] * 6)

    def test_daily_counts_use_philippine_time_and_fill_gaps(self):
        d = counts_by_day(frame())
        self.assertEqual([x.strftime("%Y-%m-%d") for x in d["date"]], ["2026-09-05", "2026-09-06", "2026-09-07"])
        # 20:00Z on Sep 5 is 04:00 on Sep 6 in Manila
        self.assertEqual(list(d["count"]), [1, 1, 2])
        self.assertEqual(d["max_magnitude"].iloc[2], 6.2)

    def test_daily_counts_zero_filled_day(self):
        d = counts_by_day(frame().iloc[[0, 2]])  # Sep 5 and Sep 7 (PH time), nothing on Sep 6
        self.assertEqual(list(d["count"]), [1, 0, 1])
        self.assertTrue(pd.isna(d["max_magnitude"].iloc[1]))

    def test_hourly_counts_cover_24_hours(self):
        h = counts_by_hour(frame())
        self.assertEqual(len(h), 24)
        self.assertEqual(int(h["count"].sum()), 4)
        self.assertEqual(int(h.loc[h["hour"] == 10, "count"].iloc[0]), 2)  # 02:32Z and 02:00Z -> 10:xx PH
        self.assertEqual(int(h.loc[h["hour"] == 4, "count"].iloc[0]), 1)

    def test_region_stats(self):
        r = region_stats(frame())
        top = r.iloc[0]
        self.assertEqual((top["region"], top["count"]), ("Pangasinan", 2))
        self.assertAlmostEqual(top["mean_magnitude"], 1.75)
        self.assertEqual(len(region_stats(frame(), top=2)), 2)
        self.assertTrue(region_stats(frame().iloc[0:0]).empty)


class ExportTests(unittest.TestCase):
    def test_csv_export_has_expected_header_and_rows(self):
        text = to_csv_bytes(frame()).decode("utf-8-sig")
        lines = text.strip().splitlines()
        self.assertTrue(lines[0].startswith("time_ph_utc+8,magnitude"))
        self.assertEqual(len(lines), 5)

    def test_excel_export_has_all_sheets(self):
        book = pd.read_excel(io.BytesIO(to_excel_bytes(frame())), sheet_name=None)
        self.assertEqual(set(book), {"Events", "Daily counts", "Magnitude ranges", "By region"})
        self.assertEqual(len(book["Events"]), 4)


if __name__ == "__main__":
    unittest.main()
