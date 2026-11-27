"""Wikipedia Live Pulse - producer.

Reads the Wikimedia EventStreams `recentchange` stream (server-sent events),
keeps only edit events, and publishes each one as JSON to a Pub/Sub topic.

- Reconnects automatically and resumes with Last-Event-ID, so no events are
  skipped after a dropped connection.
- Serves a tiny health endpoint on $PORT, which Cloud Run requires.
- DRY_RUN=1 prints events instead of publishing (no GCP needed).
- Works with the Pub/Sub emulator: set PUBSUB_EMULATOR_HOST (the client
  library picks it up automatically) and CREATE_TOPIC=1.
"""
import json
import logging
import os
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests

# Titles come in every script (Cyrillic, Chinese, ...); a Windows console would
# crash on them, so force UTF-8 output and replace anything unprintable.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "wiki-live-pulse/0.1 (student project, SRH Heidelberg)"

PROJECT_ID = os.getenv("PROJECT_ID", "")
TOPIC_ID = os.getenv("TOPIC_ID", "wiki-edits")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "0"))  # 0 = run forever
CREATE_TOPIC = os.getenv("CREATE_TOPIC", "0") == "1"
PORT = int(os.getenv("PORT", "8080"))
LOG_EVERY = int(os.getenv("LOG_EVERY", "100"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

stats = {"published": 0, "failed": 0, "reconnects": 0, "started": time.time()}
stop_event = threading.Event()


# ---------------------------------------------------------------- health check
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"status": "ok", **stats}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # keep logs quiet
        pass


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("Health endpoint on port %d", PORT)


# ---------------------------------------------------------------- publishing
class Sink:
    """Publishes events to Pub/Sub, or prints them in dry-run mode."""

    def __init__(self):
        if DRY_RUN:
            self.publisher = None
            log.info("DRY_RUN: events are printed, not published")
            return
        if not PROJECT_ID:
            raise SystemExit("PROJECT_ID is required (or set DRY_RUN=1)")
        from google.cloud import pubsub_v1  # imported here so dry runs need no GCP library

        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(PROJECT_ID, TOPIC_ID)
        if CREATE_TOPIC:
            self._ensure_topic()
        log.info("Publishing to %s", self.topic_path)

    def _ensure_topic(self):
        from google.api_core.exceptions import AlreadyExists

        try:
            self.publisher.create_topic(name=self.topic_path)
            log.info("Created topic %s", self.topic_path)
        except AlreadyExists:
            pass

    def send(self, event: dict):
        if self.publisher is None:
            print(f"[{event.get('wiki')}] bot={event.get('bot')} {event.get('title')}")
            stats["published"] += 1
            return
        data = json.dumps(event, ensure_ascii=False).encode("utf-8")
        future = self.publisher.publish(
            self.topic_path,
            data,
            wiki=str(event.get("wiki", "")),
            bot=str(bool(event.get("bot"))).lower(),
        )
        future.add_done_callback(self._on_done)

    @staticmethod
    def _on_done(future):
        try:
            future.result()
            stats["published"] += 1
        except Exception as exc:  # noqa: BLE001 - log and keep going
            stats["failed"] += 1
            log.warning("Publish failed: %s", exc)

    def close(self):
        if self.publisher is not None:
            self.publisher.stop()  # flushes pending messages


# ---------------------------------------------------------------- stream reading
def read_stream(last_event_id=None):
    """Yield (event_id, event_dict) from the SSE stream until it ends."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    # read timeout 60 s: if nothing arrives for a minute, reconnect
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as resp:
        resp.raise_for_status()
        event_id, data_lines = None, []
        for line in resp.iter_lines(decode_unicode=True):
            if stop_event.is_set():
                return
            if line is None:
                continue
            if line == "":  # blank line = end of one SSE message
                if data_lines:
                    try:
                        yield event_id, json.loads("\n".join(data_lines))
                    except json.JSONDecodeError:
                        log.warning("Skipped a malformed event")
                event_id, data_lines = None, []
            elif line.startswith("id:"):
                event_id = line[3:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())


def run():
    start_health_server()
    sink = Sink()
    last_id, backoff, count = None, 1, 0
    try:
        while not stop_event.is_set():
            try:
                for event_id, event in read_stream(last_id):
                    if event_id:
                        last_id = event_id
                    if event.get("type") != "edit":
                        continue
                    sink.send(event)
                    count += 1
                    backoff = 1
                    if count % LOG_EVERY == 0:
                        log.info("events sent: %d (failed: %d)", count, stats["failed"])
                    if MAX_EVENTS and count >= MAX_EVENTS:
                        stop_event.set()
                        break
            except (requests.RequestException, ConnectionError) as exc:
                if stop_event.is_set():
                    break
                stats["reconnects"] += 1
                log.warning("Stream error (%s); reconnecting in %ds", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
    finally:
        sink.close()
        log.info("Stopped after %d events", count)


def _handle_signal(signum, _frame):
    log.info("Signal %s received, shutting down", signum)
    stop_event.set()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_signal)  # Cloud Run / docker stop
    signal.signal(signal.SIGINT, _handle_signal)   # Ctrl+C
    run()
