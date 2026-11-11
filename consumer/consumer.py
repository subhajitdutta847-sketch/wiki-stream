import json
import os
import sqlite3
import time

from confluent_kafka import Consumer

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DB_PATH = os.getenv("DB_PATH", "../data/wiki.db")
UNUSUAL_CHARS = 1000   # an edit changing the page size by this much counts as "unusual"
KEEP_HOURS = 48        # older events are deleted so the database stays small and fast
CLEANUP_EVERY_S = 600

# ---- Database ----
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
db = sqlite3.connect(DB_PATH, timeout=30)
db.execute("PRAGMA journal_mode=WAL")  # lets the dashboard read while we write
db.execute("""CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,   -- PRIMARY KEY = the same event can only be stored once
    wiki TEXT, type TEXT, user TEXT, bot INTEGER, title TEXT,
    event_ts INTEGER, size_change INTEGER, unusual INTEGER,
    processed_ts REAL, latency_s REAL)""")
db.execute("CREATE INDEX IF NOT EXISTS idx_ts ON events(event_ts)")
db.execute("CREATE INDEX IF NOT EXISTS idx_proc ON events(processed_ts)")
db.execute("CREATE TABLE IF NOT EXISTS stats (name TEXT PRIMARY KEY, value INTEGER)")
db.execute("INSERT OR IGNORE INTO stats VALUES ('duplicates_dropped', 0)")
db.commit()

# ---- Kafka ----
consumer = Consumer({
    "bootstrap.servers": KAFKA,
    "group.id": "wiki-consumers",
    "auto.offset.reset": "earliest",   # a new consumer starts from the beginning
    "enable.auto.commit": False,       # we commit ourselves, after saving to the database
})
consumer.subscribe(["wiki-edits"])

total_new = 0
total_dups = 0
last_print = time.time()
last_cleanup = time.time()
print("Consumer started")

try:
    while True:
        msgs = consumer.consume(num_messages=500, timeout=1.0)
        if not msgs:
            continue
        now = time.time()
        dups = 0
        for m in msgs:
            if m.error():
                print("Kafka error:", m.error())
                continue
            try:
                e = json.loads(m.value())
            except json.JSONDecodeError:
                continue

            old, new = e.get("len_old"), e.get("len_new")
            change = new - old if (old is not None and new is not None) else None
            unusual = 1 if (e.get("type") == "edit" and change is not None
                            and abs(change) >= UNUSUAL_CHARS) else 0
            ts = e.get("event_ts")
            latency = now - ts if ts else None

            cur = db.execute(
                "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (e["event_id"], e.get("wiki"), e.get("type"), e.get("user"),
                 int(bool(e.get("bot"))), e.get("title"), ts, change, unusual,
                 now, latency))
            if cur.rowcount == 0:      # event_id already exists -> duplicate, ignored
                dups += 1
            else:
                total_new += 1

        if dups:
            db.execute("UPDATE stats SET value = value + ? WHERE name = 'duplicates_dropped'", (dups,))
            total_dups += dups
        db.commit()                              # 1) save to the database
        consumer.commit(asynchronous=False)      # 2) only then tell Kafka "done"

        if time.time() - last_cleanup >= CLEANUP_EVERY_S:
            db.execute("DELETE FROM events WHERE processed_ts < ?", (time.time() - KEEP_HOURS * 3600,))
            db.commit()
            last_cleanup = time.time()

        if time.time() - last_print >= 10:
            print(f"saved={total_new} duplicates_dropped={total_dups}")
            last_print = time.time()
except KeyboardInterrupt:
    print("Stopping...")
finally:
    consumer.close()
    db.close()