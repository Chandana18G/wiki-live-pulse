-- Wikipedia Live Pulse: gold layer (views the dashboard reads).
-- Every view filters on ts, so BigQuery only scans recent partitions (cheap).

-- Silver without duplicates (Pub/Sub is at-least-once).
CREATE OR REPLACE VIEW wiki.silver_edits_dedup AS
SELECT * EXCEPT (rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_ts) AS rn
  FROM wiki.silver_edits
  WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
)
WHERE rn = 1;

-- Chart 1: edits per minute, last 24 hours, bots vs humans.
CREATE OR REPLACE VIEW wiki.gold_edits_per_minute AS
SELECT
  TIMESTAMP_TRUNC(ts, MINUTE) AS minute,
  COUNT(*)                    AS edits,
  COUNTIF(is_bot)             AS bot_edits,
  COUNTIF(NOT is_bot)         AS human_edits
FROM wiki.silver_edits_dedup
WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY minute;

-- Chart 2: bot vs human share per hour, last 24 hours.
CREATE OR REPLACE VIEW wiki.gold_bot_share_hourly AS
SELECT
  TIMESTAMP_TRUNC(ts, HOUR)                     AS hour,
  COUNT(*)                                      AS edits,
  ROUND(100 * COUNTIF(is_bot) / COUNT(*), 1)    AS bot_share_pct
FROM wiki.silver_edits_dedup
WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY hour;

-- Chart 3: top 10 most-edited pages in the last hour (articles only, humans and bots).
CREATE OR REPLACE VIEW wiki.gold_top_pages_1h AS
SELECT
  wiki,
  title,
  COUNT(*)            AS edits,
  COUNTIF(is_bot)     AS bot_edits,
  SUM(bytes_changed)  AS net_bytes
FROM wiki.silver_edits_dedup
WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND namespace = 0
GROUP BY wiki, title
ORDER BY edits DESC
LIMIT 10;

-- Spike detection: z-score of each minute against the previous 60 minutes.
-- A minute is a spike when z > 3 (more than 3 standard deviations above normal).
CREATE OR REPLACE VIEW wiki.gold_spikes AS
WITH per_minute AS (
  SELECT minute, edits FROM wiki.gold_edits_per_minute
),
scored AS (
  SELECT
    minute,
    edits,
    AVG(edits)    OVER w AS mean_prev_60,
    STDDEV(edits) OVER w AS sd_prev_60
  FROM per_minute
  WINDOW w AS (ORDER BY UNIX_SECONDS(minute) RANGE BETWEEN 3600 PRECEDING AND 60 PRECEDING)
)
SELECT
  minute,
  edits,
  ROUND(mean_prev_60, 1)                                   AS mean_prev_60,
  ROUND(SAFE_DIVIDE(edits - mean_prev_60, sd_prev_60), 2)  AS z_score,
  COALESCE(SAFE_DIVIDE(edits - mean_prev_60, sd_prev_60) > 3, FALSE) AS is_spike
FROM scored;
