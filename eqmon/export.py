"""CSV / Excel export helpers."""
from __future__ import annotations

import io

import pandas as pd

from .aggregate import LOCAL_TZ, counts_by_day, magnitude_distribution, region_stats


def to_local(df: pd.DataFrame) -> pd.DataFrame:
    """Add a timezone-naive Philippine-time column (Excel cannot store tz-aware datetimes)."""
    if df.empty:
        return df.assign(time_ph=pd.Series(dtype="datetime64[ns]"))
    return df.assign(time_ph=df["event_time"].dt.tz_convert(LOCAL_TZ).dt.tz_localize(None))


_EXPORT_COLUMNS = ["time_ph", "magnitude", "depth_km", "latitude", "longitude", "location", "region", "bulletin_url", "event_id"]


def export_frame(df: pd.DataFrame) -> pd.DataFrame:
    local = to_local(df)
    return local[[c for c in _EXPORT_COLUMNS if c in local.columns]].rename(columns={"time_ph": "time_ph_utc+8"})


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return export_frame(df).to_csv(index=False).encode("utf-8-sig")  # BOM so Excel reads UTF-8


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        export_frame(df).to_excel(writer, sheet_name="Events", index=False)
        counts_by_day(df).to_excel(writer, sheet_name="Daily counts", index=False)
        magnitude_distribution(df).to_excel(writer, sheet_name="Magnitude ranges", index=False)
        region_stats(df).to_excel(writer, sheet_name="By region", index=False)
    return buffer.getvalue()
