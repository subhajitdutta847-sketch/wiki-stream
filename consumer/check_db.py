import sqlite3

c = sqlite3.connect("../data/wiki.db")
total, unusual, latency = c.execute(
    "SELECT COUNT(*), SUM(unusual), AVG(latency_s) FROM events").fetchone()
dups = c.execute("SELECT value FROM stats WHERE name='duplicates_dropped'").fetchone()[0]
print(f"events saved:       {total}")
print(f"unusual edits:      {unusual}")
print(f"avg latency (sec):  {latency:.2f}")
print(f"duplicates dropped: {dups}")