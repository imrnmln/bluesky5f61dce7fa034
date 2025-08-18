import asyncio
import websockets
import json
import random
import logging
import hashlib
import re
from typing import AsyncGenerator, Any, Dict, Optional, Tuple
from datetime import datetime, timezone
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    ExternalId,
    ExternalParentId,
    Url,
    Domain,
)

# ======================
# Config & Logging
# ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

DEFAULT_OLDNESS_SECONDS = 3600
DEFAULT_MAXIMUM_ITEMS = 20
DEFAULT_MIN_POST_LENGTH = 10
DEFAULT_SKIP_PROBABILITY = 0.1

JETSTREAM_ENDPOINTS = [
    "jetstream1.us-east.bsky.network",
    "jetstream2.us-east.bsky.network",
    "jetstream1.us-west.bsky.network",
    "jetstream2.us-west.bsky.network",
]

WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 20
WS_RECV_TIMEOUT = 25  # seconds

# Precompiled for trimming >6 fractional digits (nanoseconds -> microseconds)
_FRACTION_TRIM_RE = re.compile(
    r"^(?P<head>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.)(?P<frac>\d+)(?P<tz>Z|[+-]\d{2}:\d{2})$"
)

def read_parameters(parameters: Optional[dict]) -> Tuple[int, int, int, float]:
    p = parameters or {}
    max_oldness_seconds = p.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
    maximum_items_to_collect = p.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
    min_post_length = p.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
    skip_probability = p.get("skip_probability", DEFAULT_SKIP_PROBABILITY)
    return (
        max_oldness_seconds,
        maximum_items_to_collect,
        min_post_length,
        skip_probability,
    )

def _trim_fraction_to_6(s: str) -> str:
    """
    If fractional seconds > 6 digits, trim to 6 so Python 3.10 can parse.
    """
    m = _FRACTION_TRIM_RE.match(s)
    if not m:
        return s
    frac6 = m.group("frac")[:6]
    return f"{m.group('head')}{frac6}{m.group('tz')}"

def normalize_created_at(value: str) -> Tuple[Optional[str], Optional[datetime]]:
    """
    Accepts various RFC3339/ISO-8601 forms (with 'Z' or offsets, with/without micros),
    returns (formatted_str_matching_madtypes, aware_datetime_utc) or (None, None) on failure.
    Target format: %Y-%m-%dT%H:%M:%S[.%f]Z (Z-UTC).
    """
    if not value or not isinstance(value, str):
        return None, None
    v = value.strip()

    # If 'Z' present, make it parseable by fromisoformat by swapping to +00:00
    v_for_parse = v.replace("Z", "+00:00")
    # Trim fractional seconds to <= 6 (fromisoformat in 3.10 doesn't like >6)
    v_for_parse = _trim_fraction_to_6(v_for_parse)

    try:
        dt = datetime.fromisoformat(v_for_parse)
    except Exception:
        # Last-resort: try naive without tz, assume UTC
        try:
            dt = datetime.fromisoformat(v_for_parse.split("+")[0])
            dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None, None

    # Normalize to UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    # Always output with microseconds (allowed by the regex)
    out = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    # If you prefer dropping trailing .000000, comment the next line.
    # (regex allows either with or without fraction)
    # out = out.replace(".000000Z", "Z")

    return out, dt

def _extract_parent_rkey(reply_obj: dict) -> str:
    """
    Extract the rkey from a reply parent uri if present.
    Example uri:
    at://did:plc:abc123/app.bsky.feed.post/3lg5vh2vzis2q
    """
    try:
        uri = reply_obj["parent"]["uri"]
        if "/app.bsky.feed.post/" in uri:
            return uri.split("/app.bsky.feed.post/")[-1]
        return ""
    except Exception:
        return ""

async def connect_to_jetstream(
    min_post_length: int,
    skip_probability: float,
    max_oldness_seconds: int,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Yields validated Bluesky post events (as dicts with parsed fields).
    Includes reconnection with exponential backoff + jitter.
    Applies min length & max oldness filter.
    """
    attempt = 0
    while True:
        endpoint = random.choice(JETSTREAM_ENDPOINTS)
        uri = f"wss://{endpoint}/subscribe?wantedCollections=app.bsky.feed.post"
        try:
            logging.info(f"[Bluesky] Connecting: {uri}")
            async with websockets.connect(
                uri,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                max_size=2**20,  # 1MB
                compression=None,
            ) as ws:
                logging.info("[Bluesky] Connected to Jetstream")
                attempt = 0  # reset backoff after success

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        logging.info("[Bluesky] recv timeout; reconnecting…")
                        break

                    try:
                        event = json.loads(msg)
                    except json.JSONDecodeError:
                        continue

                    commit = event.get("commit")
                    if not commit:
                        continue
                    if commit.get("collection") != "app.bsky.feed.post":
                        continue
                    if commit.get("operation") != "create":
                        continue

                    record = commit.get("record") or {}
                    text = str(record.get("text") or "")
                    if not text or len(text) < min_post_length:
                        continue

                    # Random skip to reduce load
                    if skip_probability > 0 and random.random() < skip_probability:
                        continue

                    created_raw = record.get("createdAt") or ""
                    created_norm, created_dt = normalize_created_at(created_raw)
                    if not created_norm:
                        # Can't satisfy CreatedAt regex -> skip safely
                        continue

                    if max_oldness_seconds and created_dt:
                        age = (datetime.now(timezone.utc) - created_dt).total_seconds()
                        if age > max_oldness_seconds:
                            continue

                    did = event.get("did") or ""
                    rkey = commit.get("rkey") or ""
                    parent_rkey = ""
                    reply_obj = record.get("reply")
                    if reply_obj:
                        parent_rkey = _extract_parent_rkey(reply_obj)

                    author_sha1_hex = hashlib.sha1(did.encode("utf-8")).hexdigest()
                    url = f"https://bsky.app/profile/{did}/post/{rkey}"

                    yield {
                        "content": text,
                        "author_sha1": author_sha1_hex,
                        "created_at": created_norm,  # strictly matches madtypes regex
                        "external_id": rkey,
                        "external_parent_id": parent_rkey,
                        "url": url,
                    }

        except websockets.exceptions.ConnectionClosed as e:
            logging.info(f"[Bluesky] Connection closed: {e} — will retry")
        except asyncio.CancelledError:
            logging.info("[Bluesky] Cancelled — closing stream")
            return
        except Exception as e:
            logging.exception(f"[Bluesky] Unexpected error — will retry: {e}")

        attempt += 1
        base = min(60, 2 ** min(attempt, 6))  # cap growth
        delay = base * (0.5 + random.random() * 0.5)
        await asyncio.sleep(delay)

async def query(parameters: dict) -> AsyncGenerator[Dict[str, Any], None]:
    (
        max_oldness_seconds,
        maximum_items_to_collect,
        min_post_length,
        skip_probability,
    ) = read_parameters(parameters)

    yielded = 0
    logging.info("[Bluesky] Streaming posts in real time from Jetstream")
    logging.info("[NEW PATCH INIT 0.0.1] Lets go bluesky!")

    try:
        async for ev in connect_to_jetstream(
            min_post_length=min_post_length,
            skip_probability=skip_probability,
            max_oldness_seconds=max_oldness_seconds,
        ):
            # Build Item only when needed (after filters)
            try:
                item = Item(
                    content=Content(ev["content"]),
                    author=Author(ev["author_sha1"]),
                    created_at=CreatedAt(ev["created_at"]),  # now always ...Z
                    domain=Domain("bsky.app"),
                    external_id=ExternalId(ev["external_id"]),
                    external_parent_id=ExternalParentId(ev["external_parent_id"]),
                    url=Url(ev["url"]),
                )
            except Exception as e:
                # Defensive: if madtypes still rejects, log and continue
                logging.debug(f"[Bluesky] Skipping item due to type validation: {e}")
                continue

            yield item
            yielded += 1
            logging.info(f"[Bluesky] Yielded {yielded}/{maximum_items_to_collect}")

            if yielded >= maximum_items_to_collect:
                break

            await asyncio.sleep(0)

    except asyncio.CancelledError:
        logging.info("[Bluesky] query() cancelled — shutting down")

    logging.info(f"[Bluesky] Collected {yielded} items — session complete")
