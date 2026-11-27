"""Turn one raw Wikipedia event into a bronze row and a silver row.

Bronze = the raw event, untouched (for replay / rebuilding silver).
Silver = one typed row per edit, only the fields the analysis needs.
User names are deliberately NOT copied to silver (data minimisation, GDPR):
the analysis only needs the bot flag.
"""
import json
from datetime import datetime, timezone


def _iso(dt_string=None, epoch=None):
    """Return an ISO-8601 UTC timestamp BigQuery accepts."""
    if dt_string:
        dt = datetime.fromisoformat(dt_string.replace("Z", "+00:00"))
    elif epoch is not None:
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    else:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def event_id(event: dict):
    return (event.get("meta") or {}).get("id")


def to_bronze(event: dict, ingest_ts: str) -> dict:
    return {
        "event_id": event_id(event),
        "ingest_ts": ingest_ts,
        "payload": json.dumps(event, ensure_ascii=False),
    }


def to_silver(event: dict, ingest_ts: str):
    """Return a silver row, or None if the event is not a usable edit."""
    if event.get("type") != "edit":
        return None
    eid = event_id(event)
    if not eid:
        return None

    meta = event.get("meta") or {}
    length = event.get("length") or {}
    old, new = length.get("old"), length.get("new")
    bytes_changed = new - old if isinstance(old, int) and isinstance(new, int) else None

    return {
        "event_id": eid,
        "ts": _iso(meta.get("dt"), event.get("timestamp")),  # event time
        "wiki": event.get("wiki"),
        "domain": meta.get("domain") or event.get("server_name"),
        "title": event.get("title"),
        "namespace": event.get("namespace"),
        "is_bot": bool(event.get("bot")),
        "is_minor": bool(event.get("minor")),
        "length_old": old,
        "length_new": new,
        "bytes_changed": bytes_changed,
        "revision_new": (event.get("revision") or {}).get("new"),
        "ingest_ts": ingest_ts,  # processing time
    }
