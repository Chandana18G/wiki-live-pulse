# Wikipedia Live Pulse

Real-time analytics of Wikipedia edits on Google Cloud Platform.
Course project for *Cloud Computing and Data Architectures*, 2026.

**Team:** Chandana (ingestion, stream processing, spike detection) · Dinesh (gold SQL, dashboard, deployment)

## Question

How is Wikipedia being edited right now: how much by bots vs humans, which pages are trending,
and when do unusual spikes in edit activity happen?

## Architecture (Kappa + Medallion)

```
Wikipedia EventStreams (SSE)
   -> producer   (Docker, Cloud Run)      reads the stream, publishes edit events
   -> Pub/Sub    topic: wiki-edits        event log, 7-day retention, replay with seek
   -> processor  (Docker, Cloud Run)      bronze + silver rows, ack after write
   -> BigQuery   bronze -> silver -> gold views (incl. spike detection)
   -> dashboard  (Streamlit, Cloud Run)   edits/min, bot vs human share, top-10 pages
```

| Layer | Table / view | Content |
|---|---|---|
| Bronze | `wiki.bronze_events` | Raw JSON of every event, kept 30 days |
| Silver | `wiki.silver_edits` | One typed row per edit, partitioned by day, clustered by wiki and bot flag. No user names. |
| Silver | `wiki.silver_edits_dedup` | Silver without duplicates (Pub/Sub is at-least-once) |
| Gold | `wiki.gold_edits_per_minute` | Chart 1: edits per minute, bots vs humans |
| Gold | `wiki.gold_bot_share_hourly` | Chart 2: bot share per hour |
| Gold | `wiki.gold_top_pages_1h` | Chart 3: top 10 articles in the last hour |
| Gold | `wiki.gold_spikes` | z-score per minute vs previous 60 minutes, spike if z > 3 |

## Repository layout

| Folder | Content | Status |
|---|---|---|
| `producer/` | Reads the stream, publishes to Pub/Sub | done, tested |
| `processor/` | Pub/Sub → BigQuery bronze + silver | done, tested locally |
| `sql/` | Table DDL and gold views | done |
| `infra/` | `setup.sh`, `deploy.sh`, `teardown.sh` | done |
| `dashboard/` | Streamlit app | next (Dinesh) |
| `tests/` | Unit tests for transform and processor | 11 tests passing |
| `scripts/` | Access test, peek subscriber | done |
| `data/sample/` | 50 real sample events | done |

## Deploy to GCP (from Google Cloud Shell, no local installs)

1. Open [Cloud Shell](https://shell.cloud.google.com) and clone this repo.
2. Select your project and run the one-time setup:
   ```bash
   gcloud config set project <YOUR_PROJECT_ID>
   bash infra/setup.sh
   ```
   This enables the APIs, creates the Pub/Sub topic and subscription, the BigQuery
   dataset, tables and views, the image repository and one service account per component.
3. Build and deploy both containers (Cloud Build builds the images, no local Docker needed):
   ```bash
   bash infra/deploy.sh
   ```
4. Check that data arrives (wait a minute):
   ```bash
   bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM wiki.silver_edits'
   bq query --use_legacy_sql=false 'SELECT * FROM wiki.gold_top_pages_1h'
   ```
5. When you don't need it running, stop the always-on services: `bash infra/teardown.sh`.

## Run locally

```bash
python tests/test_transform.py && python tests/test_processor.py     # unit tests, no GCP

cd producer && pip install -r requirements.txt
DRY_RUN=1 MAX_EVENTS=20 python producer.py                            # print live edits

cd ../processor && pip install -r requirements.txt
DRY_RUN=1 SOURCE_FILE=../data/sample/sample_events.jsonl python processor.py   # transform sample
```
PowerShell: set variables with `$env:DRY_RUN=1; $env:MAX_EVENTS=20` before `python ...`.

With Docker Desktop: `docker compose up --build` runs the producer against a local Pub/Sub
emulator and a `peek` service that prints each message (health check: http://localhost:8080).

## Configuration

| Component | Variable | Default | Meaning |
|---|---|---|---|
| both | `PROJECT_ID` | (required unless dry run) | GCP project |
| both | `DRY_RUN` | `0` | `1` = print instead of publish/write |
| both | `PORT` | `8080` | Health-check port (Cloud Run needs one) |
| producer | `TOPIC_ID` | `wiki-edits` | Pub/Sub topic |
| producer | `MAX_EVENTS` | `0` | Stop after N events (`0` = forever) |
| producer | `CREATE_TOPIC` | `0` | `1` = create the topic if missing (emulator) |
| producer | `LOG_EVERY` | `100` | Progress line every N events |
| processor | `SUBSCRIPTION_ID` | `wiki-edits-processor` | Pub/Sub subscription |
| processor | `BQ_DATASET` | `wiki` | BigQuery dataset |
| processor | `BATCH_SIZE` / `FLUSH_SECONDS` | `500` / `2` | Write when either is reached |
| processor | `SOURCE_FILE` | (empty) | Read a .jsonl file instead of Pub/Sub |

## Design decisions (for the report)

- **Kappa, not Lambda:** the source is a stream; Pub/Sub retains 7 days and supports seek,
  and bronze keeps the raw events, so reprocessing never needs a second batch code path.
- **Ack after write:** the processor acknowledges messages only after BigQuery accepted them
  (at-least-once). Duplicates are removed by insert ids and by `silver_edits_dedup`.
- **Stateless processor:** spike detection runs in SQL on the gold layer, so the processor
  could scale to several instances without sharing state.
- **Cost:** serverless everywhere; partitioned tables and time-filtered views keep queries small;
  bronze partitions expire after 30 days; `teardown.sh` stops the always-on services.
- **Privacy:** user names stay in bronze only (30 days) and never reach silver or gold.

## Data source

[Wikimedia EventStreams](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams),
stream `recentchange`. Public, no API key; a descriptive User-Agent is required.
