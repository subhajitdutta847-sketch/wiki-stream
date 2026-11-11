import os
import sqlite3
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, TopicPartition

HERE = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("DB_PATH", HERE.parent / "data" / "wiki.db")).resolve()
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "wiki-edits"

# Wikimedia projects that are not encyclopedic text (page size is not a vandalism signal there)
NON_WIKIPEDIA = ("'commonswiki','wikidatawiki','metawiki','specieswiki','mediawikiwiki',"
                 "'testwiki','test2wiki','testwikidatawiki','foundationwiki','sourceswiki',"
                 "'incubatorwiki','loginwiki','outreachwiki','wikimaniawiki','donatewiki'")

st.set_page_config(page_title="Wikipedia Edit Monitor", layout="wide")
st.title("Wikipedia Edit Monitor (live)")
st.caption("Wikimedia live edits -> Kafka -> consumer -> SQLite -> this dashboard")

# ---------------- sidebar controls ----------------
scope = st.sidebar.radio("Scope", ["Wikipedia language editions", "All Wikimedia projects"])
threshold = st.sidebar.slider(
    "An edit is 'unusual' if the page size changes by at least (characters)",
    200, 5000, 1000, 100)
who = st.sidebar.radio("Editors", ["All", "Humans only", "Bots only"])
st.sidebar.caption("All numbers are cumulative over the last 48 hours of data. Refresh every 5 s.")

BOT_SQL = {"All": "", "Humans only": "AND bot = 0", "Bots only": "AND bot = 1"}[who]
WIKI_SQL = (f"AND wiki LIKE '%wiki' AND wiki NOT IN ({NON_WIKIPEDIA})"
            if scope == "Wikipedia language editions" else "")
EDITS = "type = 'edit' AND size_change IS NOT NULL AND event_ts > :since"
UNUSUAL = "ABS(size_change) >= :t"


def query(sql, params=None):
    con = sqlite3.connect(str(DB_PATH), timeout=10)
    try:
        return pd.read_sql_query(sql, con, params=params or {})
    finally:
        con.close()


@st.cache_resource
def kafka_probe():
    return Consumer({"bootstrap.servers": KAFKA, "group.id": "dashboard-probe"})


def kafka_total():
    """Total messages ever written to the topic (sum of the end offsets of all partitions)."""
    try:
        c = kafka_probe()
        parts = c.list_topics(TOPIC, timeout=3).topics[TOPIC].partitions
        return sum(c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=3)[1] for p in parts)
    except Exception:
        return None


@st.fragment(run_every=5)
def render():
    if not DB_PATH.exists():
        st.warning("No data yet. Start the producer and the consumer.")
        return
    p = {"t": threshold, "since": time.time() - 7 * 86400}

    # ---------------- headline numbers ----------------
    k = query(f"""SELECT COUNT(*) AS edits,
                         COALESCE(SUM({UNUSUAL}), 0) AS unusual,
                         AVG(ABS(size_change)) AS avg_abs,
                         MIN(event_ts) AS first_ts, MAX(event_ts) AS last_ts
                  FROM events WHERE {EDITS} {WIKI_SQL} {BOT_SQL}""", p).iloc[0]
    if k["edits"] == 0:
        st.info("Waiting for data...")
        return
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Edits analysed", f"{int(k['edits']):,}")
    c2.metric("Unusual edits", f"{int(k['unusual']):,}")
    c3.metric("Unusual share", f"{100 * k['unusual'] / k['edits']:.1f} %")
    c4.metric("Avg size change (chars)", f"{k['avg_abs']:.0f}")
    first = pd.to_datetime(k["first_ts"], unit="s").strftime("%d %b %H:%M")
    hours = (k["last_ts"] - k["first_ts"]) / 3600
    st.caption(f"Scope: {scope}. Cumulative since {first} UTC ({hours:.1f} hours of data)")

    # ---------------- objective 1 ----------------
    st.header("Objective 1: Where is risky editing concentrated?")
    left, right = st.columns([3, 2])
    wikis = query(f"""SELECT wiki, COUNT(*) AS edits, SUM({UNUSUAL}) AS unusual,
                             ROUND(100.0 * SUM({UNUSUAL}) / COUNT(*), 1) AS unusual_pct,
                             ROUND(AVG(ABS(size_change))) AS avg_size_change
                      FROM events WHERE {EDITS} {WIKI_SQL} {BOT_SQL}
                      GROUP BY wiki HAVING COUNT(*) >= 100
                      ORDER BY unusual DESC LIMIT 15""", p)
    left.subheader("Top wikis by unusual edits")
    left.dataframe(wikis, hide_index=True, column_config={
        "unusual_pct": st.column_config.ProgressColumn(
            "unusual %", min_value=0, max_value=100, format="%.1f%%")})
    trend = query(f"""SELECT strftime('%m-%d %H:00', event_ts, 'unixepoch') AS hour,
                             ROUND(100.0 * SUM({UNUSUAL}) / COUNT(*), 1) AS unusual_pct
                      FROM events WHERE {EDITS} {WIKI_SQL} {BOT_SQL}
                      GROUP BY hour ORDER BY hour""", p)
    right.subheader("Unusual share per hour (%)")
    right.bar_chart(trend.set_index("hour")["unusual_pct"])

    # ---------------- objective 2 ----------------
    st.header("Objective 2: Who and what needs review?")
    a, b, c = st.columns(3)
    users = query(f"""SELECT user, CASE MAX(bot) WHEN 1 THEN 'bot' ELSE 'human' END AS kind,
                             SUM({UNUSUAL}) AS unusual_edits, COUNT(*) AS edits
                      FROM events WHERE {EDITS} {WIKI_SQL} {BOT_SQL}
                      GROUP BY user ORDER BY unusual_edits DESC LIMIT 10""", p)
    a.subheader("Top users")
    a.dataframe(users, hide_index=True)
    pages = query(f"""SELECT wiki, title, SUM({UNUSUAL}) AS unusual_edits, COUNT(*) AS edits
                      FROM events WHERE {EDITS} {WIKI_SQL} {BOT_SQL}
                      GROUP BY wiki, title ORDER BY unusual_edits DESC LIMIT 10""", p)
    b.subheader("Top pages")
    b.dataframe(pages, hide_index=True)
    split = query(f"""SELECT CASE bot WHEN 1 THEN 'Bots' ELSE 'Humans' END AS editors,
                             COUNT(*) AS edits, SUM({UNUSUAL}) AS unusual,
                             ROUND(100.0 * SUM({UNUSUAL}) / COUNT(*), 1) AS unusual_pct
                      FROM events WHERE {EDITS} {WIKI_SQL} GROUP BY bot""", p)
    c.subheader("Bots vs humans")
    c.dataframe(split, hide_index=True)

    # ---------------- pipeline health ----------------
    st.header("Pipeline health")
    h = query("""SELECT COUNT(*) AS n, AVG(latency_s) AS lat
                 FROM events WHERE processed_ts > :since""", {"since": time.time() - 60}).iloc[0]
    saved = int(query("SELECT COUNT(*) AS n FROM events").iloc[0, 0])
    d = query("SELECT value FROM stats WHERE name = 'duplicates_dropped'")
    dups = int(d.iloc[0, 0]) if len(d) else 0
    in_kafka = kafka_total()

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Throughput (last 60 s)", f"{h['n'] / 60:.0f} events/s")
    m2.metric("Latency (last 60 s)", "-" if pd.isna(h["lat"]) else f"{h['lat']:.1f} s")
    m3.metric("Duplicates dropped", f"{dups:,}")
    if in_kafka is not None:
        processed = saved + dups
        m4.metric("Consumer lag", f"{max(in_kafka - processed, 0):,} events")
        m5.metric("Accuracy (processed / in Kafka)", f"{100 * min(processed / in_kafka, 1):.2f} %")
    else:
        m4.metric("Consumer lag", "n/a")
        m5.metric("Accuracy", "n/a")
    st.caption(f"Last refresh {time.strftime('%H:%M:%S')}")


render()