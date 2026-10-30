# Philippine Earthquake Monitoring System

Near-real-time earthquake monitoring for the Philippines, built as a streaming pipeline:

```
GarbinWx API ─► producer.py ─► Kafka ─► Spark Structured Streaming ─► MongoDB ─► Streamlit (app.py)
 (PHIVOLCS)     validate,       topic     validate, enrich,            events,      reads MongoDB only
                dedupe,                   upsert by event_id           rejected,    (never the API
                heartbeat                                              status       or Kafka)
```

| Component | Where it runs | Notes |
|---|---|---|
| Kafka 3.8 (KRaft, 1 broker) | Docker | host listener `localhost:29092`, internal `kafka:9092` |
| MongoDB 7 | Docker | `localhost:27017`, bound to 127.0.0.1, no auth (local dev only) |
| Spark 3.5.3 (Scala 2.12, Java 17) + Kafka source + MongoDB Spark Connector 10.4.0 | Docker | job: `spark/streaming_job.py` |
| `producer.py` | Windows (Python) | polls the API every 5 min by default |
| `app.py` (Streamlit) | Windows (Python) | auto-refresh every 15 s from MongoDB |

## Data source and attribution

Data comes from the **GarbinWx Public API** (`https://data.garbinwx.org/api/earthquakes.json`,
docs: <https://garbinwx.org/developer>), which republishes bulletins from **PHIVOLCS**.

* Licence: CC BY-SA 4.0 with required attribution - the dashboard footer carries it. Free for
  **non-commercial** use; GarbinWx is independent and is **not** an official warning source.
* GarbinWx asks consumers to proxy/replicate the data, cache client-side and respect strict rate
  limits. The producer therefore polls at a configurable interval (default 300 s, **hard minimum
  60 s**), uses `ETag`/`If-Modified-Since` when offered, honours `Retry-After` and `Cache-Control:
  max-age`, and backs off exponentially after failures. The dashboard's 15 s refresh only re-reads
  MongoDB; it never touches the API.
* Set `USER_AGENT` in `.env` to something that identifies you. Read the
  [terms](https://garbinwx.org/terms) before any use beyond coursework.

## How data flows

1. **Producer** fetches the feed, validates every record (timestamp, lat/lon ranges, magnitude, depth),
   normalizes to UTC, and computes `event_id = sha1(origin time | lat | lon)`. Only new or revised
   earthquakes are published (state kept in `.state/producer_seen.json`; an event is remembered only
   after Kafka acknowledged it). Each poll also publishes a small **heartbeat** message.
2. **Spark** re-validates each message, routes bad ones to `rejected_events`, enriches good ones
   (`region`, `magnitude_class`, Philippine-time date/hour) and **upserts into `events` with
   `_id = event_id`** - replays and PHIVOLCS magnitude revisions never create duplicates.
   Heartbeats and batch stats go to `pipeline_status`.
3. **MongoDB** collections: `events`, `rejected_events`, `pipeline_status`. Indexes (event_time,
   magnitude, region+time, ingested_at) are created idempotently by the dashboard and by
   `scripts/check_connectivity.py`.
4. **Dashboard** computes aggregations (magnitude ranges, daily/hourly counts, per-area statistics)
   on demand from the persisted events, so they are always consistent with the stored data.

If any stage is down the dashboard says so (red/orange/grey banner) - it never shows sample data.

## Prerequisites

* Windows 10/11 with **Docker Desktop** (WSL 2 backend), ~4 GB RAM free for the containers.
* **Python 3.10 - 3.13** on PATH (`python --version`).
* Internet access (API, Docker images, and the Spark container downloads its Kafka/Mongo connector
  JARs on first start - this can take a few minutes).

## Setup (PowerShell, from the project folder)

```powershell
cd C:\Users\Jay\OneDrive\Desktop\Philippine-Earthquake-Monitoring

python -m venv .venv
.\.venv\Scripts\Activate.ps1          # if blocked: Set-ExecutionPolicy -Scope Process Bypass
pip install -r requirements.txt

Copy-Item .env.example .env           # then edit .env: set USER_AGENT contact
```

> Tip: OneDrive can lock/sync `.venv` and `.state` slowly. Consider keeping the project outside
> OneDrive, or pausing sync while installing.

## Start everything

Run in this order (each in its own PowerShell window, venv activated in windows 2-4):

```powershell
# 1) Infrastructure + Spark job (Kafka, topic creation, MongoDB, Spark streaming job)
docker compose up -d
docker compose ps                      # kafka & mongo should be "healthy"
docker compose logs -f spark           # wait for "[batch N] upserted=..." after the producer runs (Ctrl+C leaves it running)

# 2) Optional sanity check (one API request)
python scripts\check_connectivity.py

# 3) Producer
python producer.py

# 4) Dashboard
streamlit run app.py                   # opens http://localhost:8501
```

The first producer poll publishes the whole feed (~1,750 records); Spark picks it up within ~10 s
and the dashboard fills in. `python producer.py --dry-run` fetches and validates without Kafka;
`--once` polls a single time.

## Stop / reset

```powershell
# Ctrl+C in the producer and Streamlit windows (producer flushes Kafka and saves state on exit)
docker compose down                    # stop, keep data
docker compose down -v                 # stop and DELETE Kafka, MongoDB and Spark checkpoint data
Remove-Item .state\producer_seen.json  # also do this after `down -v` so the producer re-sends everything
```

## Tests

```powershell
pip install -r requirements-dev.txt
python -m pytest -q                    # or: python -m unittest discover -s tests -t .
```

Unit tests cover normalization/validation, deduplication, aggregations, exports, API client
helpers (backoff, Retry-After, conditional requests), the producer poll loop (with fake client/Kafka),
freshness logic and MongoDB query building. They need no Docker or network.
`scripts\check_connectivity.py` is the live check for the API, Kafka and MongoDB.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Dashboard: "MongoDB is unreachable" | `docker compose ps`; start with `docker compose up -d`; check `MONGO_URI` |
| Dashboard banner grey "No producer heartbeat" | Producer never ran, or Spark is not running: `docker compose logs spark` |
| Banner orange/red "Delayed/Stale" | Producer stopped or failing - read its console; API may be rate-limiting (it backs off automatically) |
| Producer: "Kafka broker unreachable" | Docker not running or Kafka unhealthy; `docker compose logs kafka`; make sure port 29092 is free |
| Producer log: `HTTP 429` | You are being rate limited. Raise `POLL_INTERVAL_SECONDS`; do not lower it |
| Spark container restarts repeatedly | `docker compose logs spark`. First start needs internet to fetch connector JARs; check Docker's network/proxy |
| Data missing after wiping MongoDB only | Spark checkpoints think data was already processed: use `docker compose down -v` and delete `.state\producer_seen.json` |
| `pip install confluent-kafka` fails | Use Python 3.10-3.13 (64-bit) and upgrade pip: `python -m pip install -U pip` |
| Port already in use (8501/27017/29092) | Stop the other process or change the host port in `docker-compose.yml` / `streamlit run app.py --server.port 8502` |

## Project layout

```
app.py                     Streamlit dashboard (entry point)
producer.py                API -> Kafka producer
eqmon/                     shared library: config, normalize, dedupe, aggregate, freshness, export,
                           api_client, kafka_io, db
spark/streaming_job.py     Spark Structured Streaming job (runs in Docker)
docker-compose.yml         Kafka, topic init, MongoDB, Spark
scripts/check_connectivity.py
tests/                     unit tests
requirements.txt / requirements-dev.txt / .env.example
```

## Limitations

* Single-broker Kafka and MongoDB without authentication: local development setup, not production.
* `event_id` is origin time + epicentre; if PHIVOLCS revises the epicentre or time, the revised
  record is stored as a separate event.
* Earthquake data is informational only. For official warnings use PHIVOLCS / NDRRMC.
