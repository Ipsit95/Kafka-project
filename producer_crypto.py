# """
# producer_crypto.py
# ──────────────────
# Real-time data source: Binance Public WebSocket Streams
#   • URL    : wss://stream.binance.com:9443/stream
#   • Format : WebSocket (JSON frames)
#   • Auth   : NONE — public market data requires zero credentials
#   • Volume : ~10-50 trade events/sec per symbol subscribed

# Subscribes to the @trade stream for multiple symbols.
# Every event represents a real trade that just executed on Binance.

# Schema published to Kafka:
#   trade_id     int    unique trade ID from Binance
#   symbol       str    e.g. "BTCUSDT"
#   price        float  execution price in USDT
#   quantity     float  amount of base asset traded
#   value_usdt   float  price × quantity (notional USD value)
#   buyer_maker  bool   True if the buyer was the market maker
#   trade_time   str    ISO-8601 UTC timestamp of the trade
#   ingested_at  str    ISO-8601 UTC when we received it
# """

# import json
# import logging
# import signal
# import time
# from datetime import datetime, timezone
# from typing import Optional

# import websocket          # pip install websocket-client
# from kafka import KafkaProducer

# from config import (
#     KAFKA_BOOTSTRAP_SERVERS,
#     # KAFKA_TOPIC_CRYPTO,
#     BINANCE_WS_URL,
#     BINANCE_SYMBOLS,
# )

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [CRYPTO-PRODUCER] %(levelname)s %(message)s",
# )
# log = logging.getLogger(__name__)

# # ── Graceful shutdown ─────────────────────────────────────────────────────────

# _running = True
# _ws_app  = None

# def _stop(sig, frame):
#     global _running
#     log.info("Shutdown signal received…")
#     _running = False
#     if _ws_app:
#         _ws_app.close()

# signal.signal(signal.SIGINT,  _stop)
# signal.signal(signal.SIGTERM, _stop)


# # ── Kafka producer ────────────────────────────────────────────────────────────

# _producer: Optional[KafkaProducer] = None

# def get_producer() -> KafkaProducer:
#     global _producer
#     if _producer is None:
#         _producer = KafkaProducer(
#             bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#             value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
#             key_serializer=lambda k: k.encode("utf-8") if k else None,
#             acks="all",
#             retries=5,
#             linger_ms=10,
#             compression_type="gzip",
#         )
#     return _producer


# # ── WebSocket callbacks ───────────────────────────────────────────────────────

# _sent_count = 0

# def on_open(ws):
#     # Build combined stream subscription for all symbols
#     streams = [f"{sym}@trade" for sym in BINANCE_SYMBOLS]
#     sub_msg = {
#         "method": "SUBSCRIBE",
#         "params": streams,
#         "id":     1,
#     }
#     ws.send(json.dumps(sub_msg))
#     log.info("Subscribed to Binance streams: %s", streams)


# def on_message(ws, raw):
#     global _sent_count

#     try:
#         msg = json.loads(raw)
#     except json.JSONDecodeError:
#         return

#     # Binance wraps combined stream events in {"stream": "...", "data": {...}}
#     data = msg.get("data", msg)

#     # Ignore subscription ACKs and heartbeat pings
#     if data.get("e") != "trade":
#         return

#     price    = float(data["p"])
#     quantity = float(data["q"])

#     record = {
#         "trade_id":    data["t"],
#         "symbol":      data["s"],               # e.g. "BTCUSDT"
#         "price":       price,
#         "quantity":    quantity,
#         "value_usdt":  round(price * quantity, 6),
#         "buyer_maker": data["m"],               # True = sell order was the taker
#         "trade_time":  datetime.fromtimestamp(
#                            data["T"] / 1000, tz=timezone.utc
#                        ).isoformat(),
#         "ingested_at": datetime.now(timezone.utc).isoformat(),
#     }

#     producer = get_producer()
#     # producer.send(
#     #     # KAFKA_TOPIC_CRYPTO,
#     #     key=record["symbol"],
#     #     value=record,
#     # ).add_errback(lambda exc: log.error("Kafka send failed: %s", exc))

#     _sent_count += 1
#     if _sent_count % 200 == 0:
#         log.info(
#             "Published %d crypto trade events  last=%.4f %s @ $%.2f",
#             _sent_count, record["quantity"], record["symbol"], record["price"],
#         )


# def on_error(ws, error):
#     log.error("WebSocket error: %s", error)


# def on_close(ws, code, msg):
#     log.info("WebSocket closed (code=%s msg=%s)", code, msg)


# # ── Main loop with auto-reconnect ─────────────────────────────────────────────

# def run():
#     global _ws_app

#     log.info("Starting Binance crypto trade producer")
#     log.info("Symbols  : %s", BINANCE_SYMBOLS)
#     log.info("Kafka topic: '%s'", KAFKA_TOPIC_CRYPTO)

#     while _running:
#         _ws_app = websocket.WebSocketApp(
#             BINANCE_WS_URL,
#             on_open=on_open,
#             on_message=on_message,
#             on_error=on_error,
#             on_close=on_close,
#         )
#         _ws_app.run_forever(ping_interval=20, ping_timeout=10)

#         if _running:
#             log.warning("WebSocket disconnected — reconnecting in 3s…")
#             time.sleep(3)

#     if _producer:
#         _producer.flush()
#         _producer.close()

#     log.info("Crypto producer stopped. Total trades sent: %d", _sent_count)


# if __name__ == "__main__":
#     run()

