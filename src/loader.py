"""Step 5. Store the cleaned records in SQLite (data/output.db)

One table, race_results. record_id is the primary key and (season, round, driver_id)
is unique, so the same result cannot be stored twice. We insert with INSERT OR IGNORE:
rows that already exist are skipped and never overwritten

To prove this, the same CSV is loaded two times in a row. The second load must add 0 rows
and the table must look exactly the same as after the first load

Run: python -m src.loader (keeps the existing database)
    python -m src.loader --fresh (deletes the database first)
"""
import sqlite3
import sys

import pandas as pd

from src import config
from src.counts import save_counts

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS race_results (
    record_id          TEXT PRIMARY KEY,
    season             INTEGER NOT NULL,
    round              INTEGER NOT NULL,
    race_name          TEXT NOT NULL,
    race_date          TEXT NOT NULL,
    circuit_name       TEXT,
    country            TEXT,
    driver_id          TEXT NOT NULL,
    driver_name        TEXT NOT NULL,
    driver_nationality TEXT,
    constructor_id     TEXT NOT NULL,
    constructor_name   TEXT NOT NULL,
    grid               INTEGER CHECK (grid >= 0),
    position           INTEGER CHECK (position >= 1),
    points             REAL CHECK (points >= 0),
    laps               INTEGER,
    status             TEXT,
    status_group       TEXT,
    finish_time_s      REAL,
    fastest_lap_rank   INTEGER,
    avg_speed_kph      REAL,
    positions_gained   REAL,
    is_podium          INTEGER,
    is_dnf             INTEGER,
    points_share       REAL,
    UNIQUE (season, round, driver_id)
)
"""


def insert(conn, df):
# Insert all rows, return how many were really new
    df = df.copy()
    df["race_date"] = pd.to_datetime(df["race_date"]).dt.strftime("%Y-%m-%d")
    df["fastest_lap_rank"] = df["fastest_lap_rank"].astype("Int64")
    rows = df.astype(object).where(df.notna(), None).values.tolist() # NaN -> NULL

    placeholders = ", ".join("?" * len(df.columns))
    sql = f"INSERT OR IGNORE INTO race_results ({', '.join(df.columns)}) VALUES ({placeholders})"
    before = conn.total_changes
    with conn:
        conn.executemany(sql, rows)
    return conn.total_changes - before


def snapshot(conn):
    return conn.execute("SELECT * FROM race_results ORDER BY record_id").fetchall()


def run_checks(conn):
# Run every query from sql/verify_queries.sql and print the result
    queries = [q.strip() for q in config.SQL_CHECKS.read_text().split(";") if "SELECT" in q]
    for q in queries:
        comments = [line for line in q.splitlines() if line.startswith("--")]
        comment = comments[-1].lstrip("- ") if comments else "query"
        rows = conn.execute(q).fetchall()
        print(f"  {comment}: {rows if rows else 'none'}")


def main(fresh=False):
    if not config.CLEANED_CSV.exists():
        raise SystemExit("data/cleaned_data.csv not found, run the cleaner first")
    if fresh:
        config.DB_FILE.unlink(missing_ok=True)

    df = pd.read_csv(config.CLEANED_CSV)
    conn = sqlite3.connect(config.DB_FILE)
    conn.execute(CREATE_TABLE)

    added_1 = insert(conn, df)
    after_1 = snapshot(conn)
    added_2 = insert(conn, df) # same data again
    after_2 = snapshot(conn)
    same = added_2 == 0 and after_1 == after_2

    stored = len(after_2)
    print(f"Load 1 added {added_1} rows, load 2 added {added_2} rows")
    print(f"Table unchanged after second load: {same}")
    print(f"Cleaned rows: {len(df)}, rows in database: {stored}")
    print("SQL checks:")
    run_checks(conn)
    conn.close()

    save_counts("load", cleaned_rows=len(df), first_load_added=added_1, second_load_added=added_2,
                stored=stored, idempotent=same)
    if not same:
        raise SystemExit("Second load changed the table, something is wrong")


if __name__ == "__main__":
    main(fresh="--fresh" in sys.argv)
