import asyncio
import websockets
import json
import random
import logging
import hashlib
from typing import AsyncGenerator, Any, Dict, Optional, Tuple
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
        return ""  # parent uri not a post
    except Exception:
        return ""


async def connect_to_jetstream(
    min_post_length: int,
    skip_probability: float,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Yields validated Bluesky post events (as dicts with parsed fields).
    Includes reconnection with exponential backoff + jitter.
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
                max_size=2**20,  # 1MB per message cap
                compression=None,  # lower CPU; Jetstream payloads are small
            ) as ws:
                logging.info("[Bluesky] Connected to Jetstream")
                attempt = 0  # reset backoff after a good connect

                while True:
                    try:
                        # If the server goes silent, bail and reconnect
                        msg = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        logging.info("[Bluesky] recv timeout; reconnecting…")
                        break

                    try:
                        event = json.loads(msg)
                    except json.JSONDecodeError:
                        logging.debug("[Bluesky] Ignoring invalid JSON frame")
                        continue

                    # quick structural checks
                    commit = event.get("commit")
                    if not commit:
                        continue
                    if commit.get("collection") != "app.bsky.feed.post":
                        continue
                    if commit.get("operation") != "create":
                        continue

                    record = commit.get("record") or {}
                    text = str(record.get("text") or "")
                    if not text:
                        continue

                    # Randomly skip some events to reduce load
                    if skip_probability > 0 and random.random() < skip_probability:
                        continue

                    # Filter early by min length
                    if len(text) < min_post_length:
                        continue

                    did = event.get("did") or ""
                    rkey = commit.get("rkey") or ""
                    created_at = record.get("createdAt") or ""

                    # Optional parent rkey (only for replies)
                    parent_rkey = ""
                    reply_obj = record.get("reply")
                    if reply_obj:
                        parent_rkey = _extract_parent_rkey(reply_obj)

                    author_sha1_hex = hashlib.sha1(did.encode("utf-8")).hexdigest()
                    url = f"https://bsky.app/profile/{did}/post/{rkey}"

                    yield {
                        "content": text,
                        "author_sha1": author_sha1_hex,
                        "created_at": created_at,
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

        # Exponential backoff with jitter
        attempt += 1
        base = min(60, 2 ** min(attempt, 6))  # cap growth
        delay = base * (0.5 + random.random() * 0.5)
        await asyncio.sleep(delay)


async def query(parameters: dict) -> AsyncGenerator[Dict[str, Any], None]:
    (
        max_oldness_seconds,  # kept for signature compatibility (not used yet)
        maximum_items_to_collect,
        min_post_length,
        skip_probability,
    ) = read_parameters(parameters)

    yielded = 0
    logging.info("[Bluesky] Streaming posts in real time from Jetstream")

    try:
        async for ev in connect_to_jetstream(
            min_post_length=min_post_length,
            skip_probability=skip_probability,
        ):
            # Build Item only when needed (after filters)
            item = Item(
                content=Content(ev["content"]),
                author=Author(ev["author_sha1"]),
                created_at=CreatedAt(ev["created_at"]),
                domain=Domain("bsky.app"),
                external_id=ExternalId(ev["external_id"]),
                external_parent_id=ExternalParentId(ev["external_parent_id"]),
                url=Url(ev["url"]),
            )

            yield item
            yielded += 1
            logging.info(f"[Bluesky] Yielded {yielded}/{maximum_items_to_collect}")

            if yielded >= maximum_items_to_collect:
                break

            # Tiny pause to be nice on downstream pipeline (optional)
            await asyncio.sleep(0)

    except asyncio.CancelledError:
        logging.info("[Bluesky] query() cancelled — shutting down")

    logging.info(f"[Bluesky] Collected {yielded} items — session complete")
