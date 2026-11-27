-- Wikipedia Live Pulse: BigQuery dataset and tables (bronze + silver).
-- Run once:  bq query --use_legacy_sql=false < sql/01_create_tables.sql
-- (infra/setup.sh does this for you)

CREATE SCHEMA IF NOT EXISTS wiki
OPTIONS (location = 'europe-west3', description = 'Wikipedia Live Pulse');

-- BRONZE: every event exactly as received. Kept 30 days, enough to rebuild silver.
CREATE TABLE IF NOT EXISTS wiki.bronze_events (
  event_id  STRING    NOT NULL,
  ingest_ts TIMESTAMP NOT NULL,
  payload   STRING    NOT NULL   -- the raw JSON event
)
PARTITION BY DATE(ingest_ts)
OPTIONS (partition_expiration_days = 30,
         description = 'Raw Wikipedia recentchange events (bronze)');

-- SILVER: one typed row per edit. No user names (data minimisation).
CREATE TABLE IF NOT EXISTS wiki.silver_edits (
  event_id      STRING    NOT NULL,
  ts            TIMESTAMP NOT NULL,  -- event time (when the edit happened)
  wiki          STRING,              -- e.g. enwiki, dewiki, wikidatawiki
  domain        STRING,              -- e.g. en.wikipedia.org
  title         STRING,
  namespace     INT64,               -- 0 = article
  is_bot        BOOL,
  is_minor      BOOL,
  length_old    INT64,
  length_new    INT64,
  bytes_changed INT64,
  revision_new  INT64,
  ingest_ts     TIMESTAMP            -- processing time
)
PARTITION BY DATE(ts)
CLUSTER BY wiki, is_bot
OPTIONS (description = 'Cleaned Wikipedia edits (silver)');
