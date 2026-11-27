"""Wikipedia Live Pulse - processor.

Pulls edit events from the Pub/Sub subscription, turns each into a bronze row
(raw event) and a silver row (typed, cleaned), and writes them to BigQuery in
small batches. Messages are acknowledged only AFTER BigQuery accepted the
batch, so a crash never loses data (at-least-once). Duplicates are handled by
BigQuery insert ids (event id) and by the dedup view in sql/02_gold_views.sql.

The processor keeps no state, so several instances can run side by side;
spike detection happens in SQL (gold layer) on top of silver.

Modes
- normal:          read Pub/Sub, write BigQuery
- SOURCE_FILE=...  read a .jsonl file instead of Pub/Sub (testing)
- DRY_RUN=1        print rows instead of writing BigQuery
"""
import json
import logging
import os
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from transform import now_iso, to_bronze, to_silver

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PROJECT_ID = os.getenv("PROJECT_ID", "")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "wiki-edits-processor")
DATASET = os.getenv("BQ_DATASET", "wiki")
BRONZE_TABLE = os.getenv("BQ_BRONZE_TABLE", "bronze_events")
SILVER_TABLE = os.getenv("BQ_SILVER_TABLE", "silver_edits")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
SOURCE_FILE = os.getenv("SOURCE_FILE", "")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "2"))
PORT = int(os.getenv("PORT", "8080"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("processor")

stats = {"written": 0, "skipped": 0, "failed_batches": 0, "started": time.time()}
stop_event = threading.Event()


# ---------------------------------------------------------------- health check
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"status": "ok", **stats}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("Health endpoint on port %d", PORT)


# ---------------------------------------------------------------- BigQuery sink
class Sink:
    def __init__(self):
        if DRY_RUN:
            self.client = None
            log.info("DRY_RUN: rows are printed, not written")
            return
        if not PROJECT_ID:
            raise SystemExit("PROJECT_ID is required (or set DRY_RUN=1)")
        from google.cloud import bigquery

        self.client = bigquery.Client(project=PROJECT_ID)
        self.bronze = f"{PROJECT_ID}.{DATASET}.{BRONZE_TABLE}"
        self.silver = f"{PROJECT_ID}.{DATASET}.{SILVER_TABLE}"
        log.info("Writing to %s and %s", self.bronze, self.silver)

    def write(self, bronze_rows, silver_rows) -> bool:
        if self.client is None:
            for row in silver_rows:
                print(f"[{row['wiki']}] bot={row['is_bot']} {row['bytes_changed']:+} {row['title']}"
                      if row["bytes_changed"] is not None else f"[{row['wiki']}] {row['title']}")
            return True
        try:
            # row_ids = event id: BigQuery drops a repeat of the same id (best effort)
            errors = self.client.insert_rows_json(
                self.bronze, bronze_rows, row_ids=[r["event_id"] for r in bronze_rows])
            if not errors and silver_rows:
                errors = self.client.insert_rows_json(
                    self.silver, silver_rows, row_ids=[r["event_id"] for r in silver_rows])
        except Exception as exc:  # noqa: BLE001
            log.error("BigQuery insert failed: %s", exc)
            return False
        if errors:
            log.error("BigQuery rejected rows (first 3): %s", errors[:3])
            return False
        return True


# ---------------------------------------------------------------- batching
class Batcher:
    """Collects rows; flushes every FLUSH_SECONDS or BATCH_SIZE rows; acks after success."""

    def __init__(self, sink: Sink):
        self.sink = sink
        self.lock = threading.Lock()
        self.bronze, self.silver, self.messages = [], [], []

    def add(self, bronze_row, silver_row, message=None):
        with self.lock:
            self.bronze.append(bronze_row)
            if silver_row is not None:
                self.silver.append(silver_row)
            if message is not None:
                self.messages.append(message)
            full = len(self.bronze) >= BATCH_SIZE
        if full:
            self.flush()

    def flush(self):
        with self.lock:
            bronze, silver, messages = self.bronze, self.silver, self.messages
            self.bronze, self.silver, self.messages = [], [], []
        if not bronze:
            return
        ok = self.sink.write(bronze, silver)
        for m in messages:
            m.ack() if ok else m.nack()  # nack = Pub/Sub redelivers later
        if ok:
            stats["written"] += len(silver)
        else:
            stats["failed_batches"] += 1

    def run_timer(self):
        while not stop_event.wait(FLUSH_SECONDS):
            self.flush()


def handle(raw: bytes, batcher: Batcher, message=None):
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        stats["skipped"] += 1
        log.warning("Dropped a malformed message")
        if message is not None:
            message.ack()  # a broken message will never parse; don't redeliver forever
        return
    ingest = now_iso()
    silver = to_silver(event, ingest)
    if silver is None:
        stats["skipped"] += 1
    batcher.add(to_bronze(event, ingest), silver, message)


# ---------------------------------------------------------------- sources
def run_from_file(batcher: Batcher):
    with open(SOURCE_FILE, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                handle(line.encode("utf-8"), batcher)
    batcher.flush()
    log.info("File done: %d silver rows, %d skipped", stats["written"], stats["skipped"])


def run_from_pubsub(batcher: Batcher):
    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    flow = pubsub_v1.types.FlowControl(max_messages=2 * BATCH_SIZE)
    future = subscriber.subscribe(sub_path, callback=lambda m: handle(m.data, batcher, m), flow_control=flow)
    log.info("Listening on %s", sub_path)
    threading.Thread(target=batcher.run_timer, daemon=True).start()
    while not stop_event.wait(30):
        log.info("silver rows written: %d (skipped %d, failed batches %d)",
                 stats["written"], stats["skipped"], stats["failed_batches"])
    future.cancel()  # stop pulling; wait for running callbacks to finish
    try:
        future.result(timeout=30)
    except Exception:  # noqa: BLE001 - cancelled futures raise; that's expected
        pass
    batcher.flush()


def _handle_signal(signum, _frame):
    log.info("Signal %s received, shutting down", signum)
    stop_event.set()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    batcher = Batcher(Sink())
    if SOURCE_FILE:
        run_from_file(batcher)
    else:
        start_health_server()
        run_from_pubsub(batcher)
