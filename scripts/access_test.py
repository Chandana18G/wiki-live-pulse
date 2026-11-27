"""Access test for Wikipedia EventStreams (course exercise: one real call, ~50 records).

Connects to the public recentchange stream, keeps edit events,
and saves the first N to sample_events.jsonl.

Run:  pip install requests
      python access_test.py
"""
import json
import requests

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {"User-Agent": "wiki-live-pulse/0.1 (student project, SRH Heidelberg)"}
N_RECORDS = 50
OUT_FILE = "data/sample/sample_events.jsonl"  # run from the repo root


def main():
    saved = 0
    with requests.get(STREAM_URL, headers=HEADERS, stream=True, timeout=30) as resp:
        resp.raise_for_status()
        with open(OUT_FILE, "w", encoding="utf-8") as out:
            # Server-sent events: data lines start with "data: "
            for line in resp.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data: "):
                    continue
                event = json.loads(line[len("data: "):])
                if event.get("type") != "edit":
                    continue
                out.write(json.dumps(event, ensure_ascii=False) + "\n")
                saved += 1
                if saved >= N_RECORDS:
                    break
    print(f"Saved {saved} edit events to {OUT_FILE}")


if __name__ == "__main__":
    main()
