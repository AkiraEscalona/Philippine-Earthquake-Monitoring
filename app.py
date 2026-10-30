#======================
#http://localhost:8501
#======================

"""Streamlit dashboard. Entry point: `streamlit run app.py`.

The dashboard ONLY reads persisted data from MongoDB (written by the Spark job). It never
calls the GarbinWx API or Kafka directly, and it never substitutes sample data.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo.errors import PyMongoError

from eqmon import aggregate as agg
from eqmon.config import ATTRIBUTION, load_settings
from eqmon.db import (
    build_query, count_events, distinct_regions, ensure_indexes, fetch_events, fetch_status, get_client,
)
from eqmon.export import to_csv_bytes, to_excel_bytes, to_local
from eqmon.freshness import assess
from eqmon.normalize import PH_TZ

st.set_page_config(page_title="Philippine Earthquake Monitor", layout="wide")
settings = load_settings()
REFRESH = f"{settings.dashboard_refresh_s}s"

WINDOWS = {"Last 24 hours": timedelta(days=1), "Last 7 days": timedelta(days=7),
           "Last 30 days": timedelta(days=30), "Last 90 days": timedelta(days=90), "All stored data": None}
PH_CENTER = {"lat": 12.5, "lon": 122.5}


@st.cache_resource(show_spinner=False)
def get_db(uri: str, name: str):
    client = get_client(uri)
    client.admin.command("ping")  # raises if MongoDB is unreachable (failures are not cached)
    db = client[name]
    ensure_indexes(db)
    return db


def connect():
    try:
        return get_db(settings.mongo_uri, settings.mongo_db)
    except PyMongoError as exc:
        st.error(
            f"**MongoDB is unreachable** at `{settings.mongo_uri}`. No data can be shown. "
            "Start the services with `docker compose up -d` and check the README troubleshooting section.\n\n"
            f"Details: {type(exc).__name__}"
        )
        return None


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    df = agg.with_region(df)
    df = to_local(df)
    return df


# --------------------------------------------------------------------------- status
@st.fragment(run_every=REFRESH)
def status_view():
    db = connect()
    if db is None:
        return
    try:
        status = fetch_status(db)
        total = count_events(db)
    except PyMongoError as exc:
        st.error(f"Could not read pipeline status from MongoDB ({type(exc).__name__}).")
        return
    fresh = assess(status["producer"], status["spark"])
    icon = {"ok": "🟢", "delayed": "🟠", "stale": "🔴", "unknown": "⚪"}[fresh.level]
    message = f"{icon} {fresh.headline}  \n_{total:,} earthquakes stored in MongoDB._ " + " ".join(fresh.notes)
    {"ok": st.success, "delayed": st.warning, "stale": st.error, "unknown": st.warning}[fresh.level](message)


# --------------------------------------------------------------------------- charts
def map_figure(df: pd.DataFrame):
    d = df.head(5000).copy()
    d["marker_size"] = (d["magnitude"].clip(lower=0) + 1) ** 1.7
    d["time"] = d["time_ph"].dt.strftime("%Y-%m-%d %H:%M")
    fig = px.scatter_map(
        d, lat="latitude", lon="longitude", size="marker_size", color="magnitude",
        color_continuous_scale="YlOrRd", range_color=(0, max(6, float(d["magnitude"].max()))),
        hover_name="location", hover_data={"time": True, "magnitude": ":.1f", "depth_km": True,
                                           "marker_size": False, "latitude": False, "longitude": False},
        zoom=4.3, center=PH_CENTER, map_style="open-street-map", size_max=22, height=520,
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    return fig


def charts(df: pd.DataFrame, key: str):
    left, right = st.columns(2)
    dist = agg.magnitude_distribution(df)
    left.plotly_chart(px.bar(dist, x="magnitude_range", y="count", title="Events by magnitude range",
                             labels={"magnitude_range": "Magnitude", "count": "Events"}), key=f"{key}_dist")
    daily = agg.counts_by_day(df)
    right.plotly_chart(px.bar(daily, x="date", y="count", title="Events per day (PH time)",
                              labels={"date": "Date", "count": "Events"}), key=f"{key}_daily")
    left, right = st.columns(2)
    hourly = agg.counts_by_hour(df)
    left.plotly_chart(px.bar(hourly, x="hour", y="count", title="Events by hour of day (PH time)",
                             labels={"hour": "Hour", "count": "Events"}), key=f"{key}_hourly")
    scatter = df.dropna(subset=["depth_km"])
    right.plotly_chart(px.scatter(scatter, x="magnitude", y="depth_km", title="Depth vs magnitude",
                                  labels={"depth_km": "Depth (km)", "magnitude": "Magnitude"},
                                  hover_name="location", opacity=0.6).update_yaxes(autorange="reversed"),
                       key=f"{key}_depth")



def region_table(df: pd.DataFrame, key: str, suffix: str = ""):
    regions = agg.region_stats(df, top=15)
    if regions.empty:
        return
    left, right = st.columns([3, 2])
    left.plotly_chart(
        px.bar(
            regions.iloc[::-1],
            x="count",
            y="region",
            orientation="h",
            title="Most active areas (top 15)",
            labels={"count": "Events", "region": ""},
        ),
        key=f"{key}_regions{suffix}",
    )
    right.dataframe(regions.round(2), hide_index=True, width="stretch")



def kpis(df: pd.DataFrame, total_stored: int | None = None):
    s = agg.magnitude_stats(df)
    cols = st.columns(5)
    cols[0].metric("Earthquakes", f"{s['count']:,}")
    cols[1].metric("Max magnitude", "-" if s["max"] is None else f"{s['max']:.1f}")
    cols[2].metric("Mean magnitude", "-" if s["mean"] is None else f"{s['mean']:.2f}")
    cols[3].metric("Median magnitude", "-" if s["median"] is None else f"{s['median']:.2f}")
    cols[4].metric("Events M ≥ 4.0", f"{int((df['magnitude'] >= 4).sum()):,}")


EVENT_TABLE = {
    "time_ph": st.column_config.DatetimeColumn("Time (PH, UTC+8)", format="YYYY-MM-DD HH:mm"),
    "magnitude": st.column_config.NumberColumn("Mag", format="%.1f"),
    "depth_km": st.column_config.NumberColumn("Depth (km)", format="%.0f"),
    "location": "Location", "region": "Area",
    "bulletin_url": st.column_config.LinkColumn("PHIVOLCS bulletin", display_text="open"),
}
TABLE_COLUMNS = ["time_ph", "magnitude", "depth_km", "location", "region", "bulletin_url"]


# --------------------------------------------------------------------------- live tab
@st.fragment(run_every=REFRESH)
def live_view():
    db = connect()
    if db is None:
        return
    c1, c2, c3 = st.columns([2, 2, 3])
    window = c1.selectbox("Time window", list(WINDOWS), index=1, key="live_window")
    min_mag = c2.slider("Minimum magnitude", 0.0, 8.0, 0.0, 0.1, key="live_min_mag")
    try:
        region_options = distinct_regions(db)
        regions = c3.multiselect("Area / province", region_options, key="live_region_filter")
        delta = WINDOWS[window]
        start = datetime.now(timezone.utc) - delta if delta else None
        df = fetch_events(db, build_query(start=start, min_mag=min_mag or None, regions=regions or None))
    except PyMongoError as exc:
        st.error(f"Query failed ({type(exc).__name__}). The dashboard will retry on the next refresh.")
        return

    if df.empty:
        st.info("No earthquakes match the current filters (or nothing has been ingested yet). "
                "If this is a fresh setup, start the producer and the Spark job - see the README.")
        return
    df = _prepare(df)
    st.caption(f"Auto-refreshing every {settings.dashboard_refresh_s}s from MongoDB · "
               f"newest event: {df['time_ph'].max():%Y-%m-%d %H:%M} PH time")
    kpis(df)
    st.plotly_chart(map_figure(df), key="live_map")
    st.subheader("Most recent events")
    st.dataframe(df[TABLE_COLUMNS].head(25), hide_index=True, column_config=EVENT_TABLE, width="stretch")
    charts(df, "live")
    region_table(df, "live")


# --------------------------------------------------------------------------- history tab
def ph_day_bounds(start_day: date, end_day: date):
    start = datetime.combine(start_day, time.min, tzinfo=PH_TZ).astimezone(timezone.utc)
    end = datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=PH_TZ).astimezone(timezone.utc)
    return start, end


def history_view():
    db = connect()
    if db is None:
        return
    today = datetime.now(PH_TZ).date()
    with st.form("history_filters"):
        c1, c2 = st.columns(2)
        days = c1.date_input("Date range (PH time)", value=(today - timedelta(days=30), today), max_value=today)
        mag_range = c2.slider("Magnitude range", 0.0, 9.0, (0.0, 9.0), 0.1)
        c3, c4, c5 = st.columns([3, 3, 1])
        text = c3.text_input("Location contains", placeholder="e.g. Davao, Sual, Surigao")
        try:
            region_options = distinct_regions(db)
        except PyMongoError:
            region_options = []
        regions = c4.multiselect("Area / province", region_options)
        limit = c5.number_input("Max rows", 100, 50_000, 10_000, 1_000)
        submitted = st.form_submit_button("Search history", type="primary")

    if submitted:
        if not isinstance(days, tuple) or len(days) != 2:
            st.warning("Please pick both a start and an end date.")
        else:
            start, end = ph_day_bounds(*days)
            try:
                found = fetch_events(db, build_query(start, end, mag_range[0], mag_range[1], text, regions or None), int(limit))
                st.session_state["history_df"] = _prepare(found)
                st.session_state["history_limited"] = len(found) >= int(limit)
            except PyMongoError as exc:
                st.error(f"Query failed ({type(exc).__name__}).")

    df = st.session_state.get("history_df")
    if df is None:
        st.info("Choose filters and press **Search history**.")
        return
    if df.empty:
        st.warning("No stored earthquakes match these filters.")
        return
    if st.session_state.get("history_limited"):
        st.warning("Result hit the row limit - narrow the filters or raise 'Max rows' to see everything.")
    kpis(df)
    d1, d2, _ = st.columns([1, 1, 4])
    stamp = datetime.now(PH_TZ).strftime("%Y%m%d_%H%M")
    d1.download_button("⬇ CSV", to_csv_bytes(df), f"earthquakes_{stamp}.csv", "text/csv")
    d2.download_button("⬇ Excel", to_excel_bytes(df), f"earthquakes_{stamp}.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.dataframe(df[TABLE_COLUMNS], hide_index=True, column_config=EVENT_TABLE, width="stretch")
    charts(df, "hist")
    region_table(df, "hist")


# --------------------------------------------------------------------------- layout
st.title("Philippine Earthquake Monitor")
st.caption("Pipeline: GarbinWx API → Python producer → Kafka → Spark Structured Streaming → MongoDB → this dashboard")
status_view()
tab_live, tab_history = st.tabs(["Live dashboard", "History & export"])
with tab_live:
    live_view()
with tab_history:
    history_view()
st.divider()
st.caption(ATTRIBUTION)
