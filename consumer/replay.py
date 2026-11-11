import os
import time

from confluent_kafka import Consumer, Producer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
N = int(os.getenv("REPLAY_COUNT", "500"))

# Reads N old events from Kafka and sends them again = simulates a source replaying records
consumer = Consumer({
    "bootstrap.servers": KAFKA,
    "group.id": f"replay-{int(time.time())}",   # separate group, does not touch the real consumer
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe(["wiki-edits"])
producer = Producer({"bootstrap.servers": KAFKA})

sent = 0
empty_polls = 0
while sent < N and empty_polls < 10:
    m = consumer.poll(1.0)
    if m is None:
        empty_polls += 1
        continue
    if m.error():
        continue
    producer.produce("wiki-edits", key=m.key(), value=m.value())
    sent += 1

producer.flush()
consumer.close()
print(f"Re-sent {sent} old events (duplicates)")