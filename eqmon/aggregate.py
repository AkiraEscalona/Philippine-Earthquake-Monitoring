"""Aggregation helpers for the dashboard. Operate on a DataFrame of persisted events.

Expected columns: event_time (tz-aware UTC), magnitude, depth_km, latitude, longitude,
location and (optionally) region.
"""
from __future__ import annotations

import re

import pandas as pd

LOCAL_TZ = "Asia/Manila"

# Keep in sync with spark/streaming_job.py (magnitude_class).
MAG_LABELS = ["<2", "2-3", "3-4", "4-5", "5-6", ">=6"]
_MAG_EDGES = [float("-inf"), 2, 3, 4, 5, 6, float("inf")]

# Keep in sync with the regexp used in spark/streaming_job.py.
_REGION_RE = re.compile(r"\(([^()]*)\)\s*$")


def extract_region(location: str | None) -> str:
    """Province/area = the last parenthesised group of a PHIVOLCS location string."""
    if not location:
        return "Unknown"
    match = _REGION_RE.search(location.strip())
    region = match.group(1).strip() if match else ""
    return region or "Unknown"


def magnitude_class(magnitude: float) -> str:
    for label, upper in zip(MAG_LABELS, _MAG_EDGES[1:]):
        if magnitude < upper:
            return label
    return MAG_LABELS[-1]


def with_region(df: pd.DataFrame) -> pd.DataFrame:
    if "region" in df.columns or df.empty:
        return df
    return df.assign(region=df["location"].map(extract_region))


def magnitude_stats(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"count": 0, "mean": None, "median": None, "min": None, "max": None, "std": None}
    mag = df["magnitude"]
    return {
        "count": int(len(df)),
        "mean": float(mag.mean()),
        "median": float(mag.median()),
        "min": float(mag.min()),
        "max": float(mag.max()),
        "std": float(mag.std(ddof=0)),
    }


def magnitude_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Events per magnitude range; every range is present, even with zero events."""
    if df.empty:
        counts = pd.Series(0, index=MAG_LABELS)
    else:
        binned = pd.cut(df["magnitude"], bins=_MAG_EDGES, labels=MAG_LABELS, right=False)
        counts = binned.value_counts().reindex(MAG_LABELS).fillna(0).astype(int)
    return pd.DataFrame({"magnitude_range": MAG_LABELS, "count": counts.to_numpy()})


def counts_by_day(df: pd.DataFrame, tz: str = LOCAL_TZ) -> pd.DataFrame:
    """Daily event counts in local time. Days without events are included with count 0."""
    cols = ["date", "count", "max_magnitude", "mean_magnitude"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    day = df["event_time"].dt.tz_convert(tz).dt.tz_localize(None).dt.normalize()
    grouped = (
        df.assign(day=day)
        .groupby("day")
        .agg(count=("magnitude", "size"), max_magnitude=("magnitude", "max"), mean_magnitude=("magnitude", "mean"))
    )
    grouped = grouped.reindex(pd.date_range(grouped.index.min(), grouped.index.max(), freq="D"))
    grouped["count"] = grouped["count"].fillna(0).astype(int)
    grouped.index.name = "date"
    return grouped.reset_index()[cols]


def counts_by_hour(df: pd.DataFrame, tz: str = LOCAL_TZ) -> pd.DataFrame:
    """Events per local hour of day (0-23); all 24 hours are present."""
    hours = pd.Series(range(24), name="hour")
    if df.empty:
        counts = pd.Series(0, index=range(24))
    else:
        counts = df["event_time"].dt.tz_convert(tz).dt.hour.value_counts().reindex(range(24)).fillna(0).astype(int)
    return pd.DataFrame({"hour": hours, "count": counts.to_numpy()})


def region_stats(df: pd.DataFrame, top: int | None = None) -> pd.DataFrame:
    cols = ["region", "count", "mean_magnitude", "max_magnitude", "mean_depth_km"]
    if df.empty:
        return pd.DataFrame(columns=cols)
    df = with_region(df)
    out = (
        df.groupby("region")
        .agg(
            count=("magnitude", "size"),
            mean_magnitude=("magnitude", "mean"),
            max_magnitude=("magnitude", "max"),
            mean_depth_km=("depth_km", "mean"),
        )
        .reset_index()
        .sort_values(["count", "max_magnitude"], ascending=[False, False], kind="stable")
        .reset_index(drop=True)
    )
    return out.head(top) if top else out
