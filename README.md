# Wikipedia Edit Monitor: real-time streaming pipeline with Kafka

A live dashboard that monitors edits on Wikipedia in real time and shows where unusual editing is concentrated, so a (volunteer) moderation team knows where to look first.

**Pipeline:** Wikimedia EventStreams -> Producer -> **Kafka** -> Consumer -> SQLite -> Streamlit dashboard

Everything runs with one command through Docker Compose, on Windows, macOS (Intel and Apple Silicon) and Linux.

\---

## 1\. Run it (3 steps)

**Requirements:** Docker Desktop (running) and an internet connection. Ports **8501** and **9092** must be free.

```bash
git clone https://github.com/subhajitdutta847-sketch/wiki-stream.git
cd wiki-stream
docker compose up -d --build
```

Then open **http://localhost:8501**.

* The first build downloads images and packages and takes a few minutes.
* The dashboard starts empty. After 5 to 10 minutes all sections are filled, and the numbers keep changing every 5 seconds.
* Check the services: `docker compose ps` (Kafka should be `healthy`; `init-topic` shows `Exited`, which is correct: it only creates the topic).
* Stop: `docker compose down`. Stop and delete all stored data: `docker compose down -v`.

\---

## 2\. Business objectives

Wikipedia has thousands of edits per minute. Moderators are volunteers with limited time, so they do not need a ticker. They need to know **where to look first**. The dashboard shows cumulative insights over the last 48 hours:

**Objective 1: Where is risky editing concentrated?**
Per wiki: number of edits, share of "unusual" edits, average edit size, and the unusual share per hour.

**Objective 2: Who and what needs review?**
Top 10 users and top 10 pages by number of unusual edits, plus the bot vs. human split.

**Definition:** an edit is *unusual* if the page size changes by 1,000 characters or more (mass deletion or mass insertion). The threshold can be changed in the sidebar.

**Scope:** by default only Wikipedia language editions are analysed. On Wikimedia Commons and Wikidata, page size is structured data, so large size changes are normal there and are not a vandalism signal. They can be included via the sidebar ("All Wikimedia projects").

The dashboard also has a **Pipeline health** section (throughput, latency, duplicates dropped, consumer lag, accuracy).

\---

## 3\. Architecture

```
Wikimedia EventStreams (SSE, public, no API key)
        |
   producer.py   reads the live stream, sends each edit to Kafka (auto-reconnect)
        |
   Kafka topic "wiki-edits" (3 partitions, 24 h retention)
        |
   consumer.py   deduplicates by event ID, computes size change / unusual flag,
        |        saves to SQLite, commits the Kafka offset only AFTER saving
        |
   SQLite (/data/wiki.db, last 48 h)
        |
   dashboard.py  Streamlit, queries SQLite every 5 s, reads Kafka offsets for lag
```

|Container|Role|
|-|-|
|`kafka`|Kafka broker (KRaft mode, no ZooKeeper)|
|`init-topic`|Creates the topic `wiki-edits` with 3 partitions, then exits|
|`producer`|Wikimedia stream -> Kafka|
|`consumer`|Kafka -> SQLite|
|`dashboard`|Live dashboard on port 8501|

\---

## 4\. The data source is real and live

Source: https://stream.wikimedia.org/v2/stream/recentchange (Wikimedia EventStreams, Server-Sent Events). Every edit made on any Wikimedia project appears in it within seconds.

Ways to verify it is a real, continuous stream (not a batch, not synthetic):

1. `curl -N https://stream.wikimedia.org/v2/stream/recentchange` never finishes; events keep arriving. (`curl -I` shows `Content-Type: text/event-stream`.)
2. Each event's `timestamp` is within seconds of the current time.
3. I made edits on my own Wikipedia sandbox page and watched them appear in the stream (screenshots in `docs/`).

\---

## 5\. Professor's requirements and how they are met

|Requirement|How it is met|
|-|-|
|Real, non-synthetic live source|Wikimedia EventStreams|
|Kafka between producer and consumer|Topic `wiki-edits`, consumer group `wiki-consumers`|
|Meaningful, cumulative business objectives|Objectives 1 and 2 above|
|**Downtime:** data produced while the consumer is down is not lost|Kafka stores the events. The consumer commits its offset only after saving, so after a restart it continues exactly where it stopped.|
|**Duplicates:** the same record sent several times is processed exactly once|Every event has a unique ID (`meta.id`) used as the PRIMARY KEY (`INSERT OR IGNORE`). A duplicate is ignored and counted.|

### Pipeline measures

|Measure|How it is calculated (dashboard, Pipeline health)|
|-|-|
|Throughput|Events saved in the last 60 s divided by 60|
|Latency|Processing time minus the event's own timestamp, averaged over the last 60 s|
|Scalability|Topic has 3 partitions, so up to 3 consumers in the same group can share the load (`docker compose up -d --scale consumer=3`)|
|Reliability|Producer reconnects automatically (Wikimedia closes the connection after about 15 min); Kafka acknowledgements (`acks=all`, idempotent producer); containers restart automatically; offsets committed after saving|
|Accuracy|(saved + duplicates dropped) divided by total messages written to Kafka, target 100 %|

Exactly-once effect = at-least-once delivery (offset committed after the database write) + idempotent write (event ID as primary key).

\---

## 6\. Demo scripts

**A) Consumer downtime and recovery** (watch Pipeline health on the dashboard)

```bash
docker compose stop consumer      # wait 30-60 s: throughput 0, consumer lag climbs
docker compose start consumer     # lag drops back to 0, throughput spikes while catching up
```

**B) Duplicate records** (re-sends 500 old events to Kafka)

```bash
docker compose run --rm -e KAFKA\_BOOTSTRAP=kafka:19092 consumer python consumer/replay.py
```

"Duplicates dropped" goes up by about 500 and the event totals do not change.

Evidence screenshots: `docs/`.

\---

## 7\. Project structure

```
docker-compose.yml     all services
Dockerfile             one Python image used by producer, consumer and dashboard
requirements.txt
producer/producer.py   Wikimedia -> Kafka
consumer/consumer.py   Kafka -> SQLite (dedup, 48 h cleanup)
consumer/replay.py     duplicate test: re-sends old events
dashboard/dashboard.py Streamlit dashboard
prompts.txt            AI prompts used during development
```

Settings in the code: `UNUSUAL\_CHARS` (1000), `KEEP\_HOURS` (48) in `consumer.py`; Kafka retention (24 h) in `docker-compose.yml`.

\---

## 8\. Limitations

* Single Kafka broker and SQLite are chosen for simplicity and easy reproduction, not for production scale.
* The wiki database keeps 48 hours of data. Pipeline-health counters (lag, accuracy) assume the pipeline has been running for less than 48 hours.
* "Unusual" is a simple size-change rule. It is a signal to review, not proof of vandalism.

\---

## 9\. AI tool usage

Parts of this project were developed with Claude (Anthropic). All prompts are listed in `prompts.txt`.

