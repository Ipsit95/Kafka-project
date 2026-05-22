
import json
import logging
import signal
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer


from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_WIKIPEDIA,
    # KAFKA_TOPIC_CRYPTO,
    KAFKA_GROUP_ID,
    POSTGRES,
    CONSUMER_POLL_TIMEOUT_MS,
    CONSUMER_MAX_POLL_RECORDS,
)


with open("queries.sql") as f:
    INSERT_WIKIPEDIA = f.read()


# INSERT_WIKIPEDIA = load_sql("queries.sql")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)
_running = True


# ── PostgreSQL helpers ────────────────────────────────────────────────────────

@contextmanager
def pg_connection():
    conn = psycopg2.connect(**POSTGRES)
    conn.autocommit = False
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def insert_wikipedia_batch(conn, records: list[dict]) -> int:
    if not records:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, INSERT_WIKIPEDIA, records, page_size=100)
        n = cur.rowcount
    conn.commit()
    return n

# ── Consumer ──────────────────────────────────────────────────────────────────

def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC_WIKIPEDIA,
        # KAFKA_TOPIC_CRYPTO,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        # auto_offset_reset="latest",    
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        max_poll_records=CONSUMER_MAX_POLL_RECORDS,
    )


def run():
    log.info(
        "Consumer starting  topics=[%s, %s]  group='%s'",
        KAFKA_TOPIC_WIKIPEDIA, KAFKA_GROUP_ID
        # KAFKA_TOPIC_WIKIPEDIA, KAFKA_TOPIC_CRYPTO, KAFKA_GROUP_ID,
    )

    consumer = build_consumer()
    # totals   = {KAFKA_TOPIC_WIKIPEDIA: 0, KAFKA_TOPIC_CRYPTO: 0}
    totals   = {KAFKA_TOPIC_WIKIPEDIA: 0}

    with pg_connection() as conn:
        while _running:
            msg_pack = consumer.poll(timeout_ms=CONSUMER_POLL_TIMEOUT_MS)

            if not msg_pack:
                time.sleep(0.1)
                continue

            wiki_records   = []
            # crypto_records = []

            for tp, messages in msg_pack.items():
                for msg in messages:
                    if tp.topic == KAFKA_TOPIC_WIKIPEDIA:
                        wiki_records.append(msg.value)
                    # elif tp.topic == KAFKA_TOPIC_CRYPTO:
                    #     crypto_records.append(msg.value)

            try:
                wiki_inserted   = insert_wikipedia_batch(conn, wiki_records)
                # crypto_inserted = insert_crypto_batch(conn, crypto_records)

                # Only commit Kafka offsets after both DB writes succeed
                consumer.commit()

                totals[KAFKA_TOPIC_WIKIPEDIA] += len(wiki_records)
                # totals[KAFKA_TOPIC_CRYPTO]    += len(crypto_records)

                # if wiki_records or crypto_records:
                if wiki_records:
                    log.info(
                        "Batch  wiki=%d(+%d new)  | cumulative wiki=%d",
                        len(wiki_records), wiki_inserted,
                        totals[KAFKA_TOPIC_WIKIPEDIA],
                    )

            except Exception as exc:
                log.error("DB write failed — offsets NOT committed: %s", exc)
                time.sleep(1)

    consumer.close()
    log.info(
        "Consumer stopped.  Final totals — wiki=%d", 
        totals[KAFKA_TOPIC_WIKIPEDIA], 
        # totals[KAFKA_TOPIC_CRYPTO],
    )


def test_db_connection():
    try:
        with pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("select * from sample_first_schema.wikipedia_creats;")
                version = cur.fetchone()
                print(f"version: {version}")
                print(f"PostgreSQL connection successful. Server version: {version}")
    except Exception as e:
        print(f"Database connection failed: {e}")


if __name__ == "__main__":
    run()
    # test_db_connection()


