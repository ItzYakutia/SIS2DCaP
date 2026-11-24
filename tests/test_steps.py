"""Small checks that run without Kafka or internet

Uses the real data/raw_data.csv (run the pipeline once first) and a temporary database
Run: python tests/test_steps.py
"""
import sqlite3
import sys
import tempfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src import cleaner, config, loader, validator  # noqa: E402


def sample():
    return pd.read_csv(config.RAW_CSV, dtype=str).head(200)


def test_real_rows_pass_and_broken_rows_fail():
    df = sample()
    valid, invalid = validator.split_valid_invalid(df)
    assert len(valid) == len(df) and len(invalid) == 0

    tests = validator.make_test_rows(df.iloc[0]).drop(columns="what_we_broke")
    _, caught = validator.split_valid_invalid(tests)
    assert len(caught) == 4


def test_duplicate_is_removed_in_cleaning():
    df = sample()
    doubled = pd.concat([df, df.head(5)])
    cleaned = cleaner.clean(doubled)
    assert len(cleaned) == len(df)
    assert set(cleaned["status_group"]) <= {"Finished", "Lapped", "Retired", "Disqualified"}


def test_loading_twice_adds_nothing():
    cleaned = cleaner.clean(sample())
    with tempfile.TemporaryDirectory() as tmp:
        conn = sqlite3.connect(Path(tmp) / "test.db")
        conn.execute(loader.CREATE_TABLE)
        first = loader.insert(conn, cleaned)
        before = loader.snapshot(conn)
        second = loader.insert(conn, cleaned)
        assert first == len(cleaned) and second == 0
        assert loader.snapshot(conn) == before
        conn.close()


if __name__ == "__main__":
    test_real_rows_pass_and_broken_rows_fail()
    test_duplicate_is_removed_in_cleaning()
    test_loading_twice_adds_nothing()
    print("all tests passed")
