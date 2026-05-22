# Real-Time Kafka Pipeline — Wikipedia + Crypto → PostgreSQL

```
Wikimedia SSE ──┐
                ├─→ Kafka topics ──→ consumer.py ──→ PostgreSQL
Binance WS   ──┘
```

---

## Data Sources

| Source | Protocol | Auth | Volume |
|--------|----------|------|--------|
| **Wikimedia EventStream** | SSE (HTTP long-poll) | None | ~50–200 edits/sec worldwide |
| **Binance Public WebSocket** | WebSocket | None | ~10–50 trades/sec per symbol |

---

## Prerequisites (local installs, no Docker)

### 1 — Apache Kafka (local)

```bash
# Download Kafka (adjust version as needed)
curl -O https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz
cd kafka_2.13-3.7.0

# Terminal A — start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal B — start Kafka broker
bin/kafka-server-start.sh config/server.properties

# Terminal C — create topics
bin/kafka-topics.sh --create --topic wikipedia_edits --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic crypto_trades   --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 2 — PostgreSQL (local)

Install from https://www.postgresql.org/download/ then:

```sql
-- run as superuser (psql -U postgres)
CREATE DATABASE streams_db;
```

Update `config.py` with your local PostgreSQL username and password.

### 3 — Python dependencies

```bash
pip install -r requirements.txt
```

---

## Running the Pipeline

Open **four terminals**:

```bash
# Terminal 1 — Wikipedia producer (SSE stream)
python producer_wikipedia.py

# Terminal 2 — Crypto producer (Binance WebSocket)
python producer_crypto.py

# Terminal 3 — Consumer (loads both topics to PostgreSQL)
python consumer.py

# Terminal 4 — (optional) watch live data in PostgreSQL
watch -n 2 'psql -U postgres -d streams_db -c "
  SELECT COUNT(*) AS wiki_edits FROM wikipedia_edits;
  SELECT COUNT(*) AS crypto_trades FROM crypto_trades;
  SELECT symbol, COUNT(*), ROUND(AVG(price)::numeric,2) avg_price
  FROM crypto_trades GROUP BY symbol ORDER BY 2 DESC;
"'
```

---

## Database Tables

### `wikipedia_edits`
| Column | Type | Description |
|--------|------|-------------|
| event_id | TEXT UNIQUE | Wikimedia event ID |
| wiki | VARCHAR | e.g. "enwiki", "dewiki", "wikidata" |
| title | TEXT | Page that was edited |
| editor | TEXT | Username or IP address |
| edit_type | VARCHAR | edit / new / categorize / log |
| is_bot | BOOLEAN | Automated bot edit |
| bytes_delta | INT | Size change in bytes |
| comment | TEXT | Editor's summary |
| page_url | TEXT | Canonical URL |
| edit_ts | TIMESTAMPTZ | When the edit happened |

### `crypto_trades`
| Column | Type | Description |
|--------|------|-------------|
| trade_id | BIGINT UNIQUE | Binance trade ID |
| symbol | VARCHAR | e.g. "BTCUSDT" |
| price | NUMERIC | Execution price |
| quantity | NUMERIC | Base asset amount |
| value_usdt | NUMERIC | Notional USD value |
| buyer_maker | BOOLEAN | True = sell was taker |
| trade_ts | TIMESTAMPTZ | Trade execution time |

---

## Key Design Decisions

| Feature | Detail |
|---------|--------|
| **Manual Kafka commits** | Offsets advance only after a successful PostgreSQL write — no silent data loss |
| **Idempotent inserts** | `ON CONFLICT … DO NOTHING` handles Kafka redeliveries safely |
| **Auto-reconnect** | Both producers reconnect automatically on network drops |
| **Micro-batching** | `max_poll_records=50` + `execute_batch` keeps DB writes efficient |
| **Dual-topic consumer** | One consumer group subscribes to both topics simultaneously |

---

## Useful Kafka CLI commands

```bash
# Watch Wikipedia topic live
bin/kafka-console-consumer.sh --topic wikipedia_edits --bootstrap-server localhost:9092 --from-beginning

# Watch crypto topic live
bin/kafka-console-consumer.sh --topic crypto_trades --bootstrap-server localhost:9092

# Consumer group lag
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group postgres_loader_group
```
