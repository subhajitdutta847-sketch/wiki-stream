import json
import os
import time

import requests
from confluent_kafka import Producer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "wiki-edits"
URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {
    "User-Agent": "SRH-Student-Streaming-Project/1.0 (university project)",
    "Accept": "text/event-stream",
}

producer = Producer({
    "bootstrap.servers": KAFKA,
    "acks": "all",
    "enable.idempotence": True,  # Kafka itself avoids duplicates on producer retries
    "linger.ms": 50,
})

received = 0    # events read from Wikimedia
delivered = 0   # events confirmed by Kafka
reconnects = 0  # times the stream was re-opened


def on_delivery(err, msg):
    """Kafka calls this for every message: confirms it was stored."""
    global delivered
    if err is None:
        delivered += 1
    else:
        print("Delivery failed:", err)


def handle(line):
    """Turn one SSE line into a small JSON message and send it to Kafka."""
    global received
    if not line.startswith(b"data: "):
        return
    try:
        e = json.loads(line[6:])
    except json.JSONDecodeError:
        return
    meta = e.get("meta") or {}
    length = e.get("length") or {}
    msg = {
        "event_id": meta.get("id"),      # unique ID, used to drop duplicates
        "wiki": e.get("wiki"),
        "type": e.get("type"),
        "user": e.get("user"),
        "bot": e.get("bot"),
        "title": e.get("title"),
        "event_ts": e.get("timestamp"),  # Unix seconds, used for latency
        "len_old": length.get("old"),
        "len_new": length.get("new"),
    }
    if not msg["event_id"]:
        return
    received += 1
    producer.produce(TOPIC, key=str(msg["wiki"]), value=json.dumps(msg), on_delivery=on_delivery)
    producer.poll(0)
    if received % 100 == 0:
        print(f"received={received} delivered={delivered} reconnects={reconnects}")


while True:
    try:
        with requests.get(URL, headers=HEADERS, stream=True, timeout=30) as r:
            r.raise_for_status()
            print("Connected to Wikimedia stream")
            for line in r.iter_lines():
                handle(line)
    except Exception as ex:
        print("Stream error:", ex)
    # Wikimedia closes the connection after ~15 min, so we reconnect
    reconnects += 1
    print(f"Reconnecting (#{reconnects}) in 3 s...")
    time.sleep(3)