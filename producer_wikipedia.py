import json
import logging
import signal
import time
from typing import Optional

import httpx
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_WIKIPEDIA,
    WIKIMEDIA_SSE_URL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WIKI-PRODUCER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)
_running = True

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=1,
        linger_ms=20,
        compression_type="gzip",
    )


def parse_wiki_event(raw_data: str) -> Optional[dict]:
    try:
        ev = json.loads(raw_data)
    except json.JSONDecodeError:
        return None

    ev_type = ev.get("type")
    meta    = ev.get("meta", {})
    length  = ev.get("length", {})
    old_len = length.get("old") or 0
    new_len = length.get("new") or 0

    if ev_type in ("new") and ev.get("bot") is False and ev.get("wiki", "").endswith("enwiki"):
        return {
            "event_id":    meta.get("id", ""),
            "wiki":        ev.get("wiki", ""),
            "title":       ev.get("title", ""),
            "editor":      ev.get("user", ""),
            "edit_type":   ev_type,
            "is_bot":      ev.get("bot"),
            "bytes_delta": new_len - old_len,
            "comment":     ev.get("comment", ""),
            "page_url":    meta.get("uri", ""),
            "timestamp":   meta.get("dt", ""),
        }
    return None


def run():
    producer = build_producer()
    sent = 0

    log.info("Starting — topic: '%s'", KAFKA_TOPIC_WIKIPEDIA)

    while _running:
        try:
            with httpx.stream(
                "GET",
                WIKIMEDIA_SSE_URL,
                headers={
                    "Accept": "text/event-stream",
                    "User-Agent": "KafkaPipeline/1.0 python-httpx",
                },
                timeout=30,
            ) as resp:
                resp.raise_for_status()

                data = None
                for line in resp.iter_lines():
                    if not _running:
                        break
                    if line is None:
                        break

                    if line.startswith("data:"):
                        data = line[5:].lstrip()
                    elif not line.strip() and data:
                        record = parse_wiki_event(data)
                        if record is None:
                            data = None
                            continue

                        key = f"{record['wiki']}:{record['title']}"
                        producer.send(
                            KAFKA_TOPIC_WIKIPEDIA,
                            key=key,
                            value=record,
                        ).add_errback(lambda exc: log.error("Send failed: %s", exc))

                        sent += 1
                        if sent % 500 == 0:
                            log.info("Published %d events", sent)

                        data = None

        except httpx.RequestError as exc:
            log.warning("Connection lost: %s — retrying in 5s", exc)
            time.sleep(5)
        except Exception as exc:
            log.error("Unexpected error: %s — retrying in 5s", exc)
            time.sleep(5)

    producer.flush()
    producer.close()
    log.info("Stopped. Total sent: %d", sent)


if __name__ == "__main__":
    run()