import os



# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_WIKIPEDIA   = os.getenv("KAFKA_TOPIC_WIKIPEDIA", "wikipedia_creats")
KAFKA_TOPIC_CRYPTO      = os.getenv("KAFKA_TOPIC_CRYPTO")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "postgres_loader_group")
CONSUMER_POLL_TIMEOUT_MS  = int(os.getenv("KAFKA_CONSUMER_POLL_TIMEOUT_MS", "1000"))
CONSUMER_MAX_POLL_RECORDS = int(os.getenv("KAFKA_CONSUMER_MAX_POLL_RECORDS", "50"))

# ── PostgreSQL ────────────────────────────────────────────────────────────────
POSTGRES = {
    "host":     os.getenv("PG_HOST"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DBNAME"),
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}

# ── Wikimedia SSE ─────────────────────────────────────────────────────────────
WIKIMEDIA_SSE_URL = os.getenv("WIKIMEDIA_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")

# ── Binance WebSocket ─────────────────────────────────────────────────────────
BINANCE_WS_URL  = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream")
BINANCE_SYMBOLS = os.getenv("BINANCE_SYMBOLS", "btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt").split(",")