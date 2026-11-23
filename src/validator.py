"""Step 3. Check the records from Kafka with a Pandera schema, before any cleaning

Rows that break a rule are not fixed or dropped silently: they go to
data/invalid_records.csv together with the reason. Only valid rows move on

Our real data had no bad rows, so we also build 4 broken test rows on purpose
and show that the schema catches all of them. They are validated on their own,
so they never get into the real dataset

Run: python -m src.validator
"""
from datetime import date

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from src import config
from src.counts import save_counts

THIS_YEAR = date.today().year

schema = pa.DataFrameSchema(
    {
        # id looks like 2024-5-hamilton and must not repeat
        "record_id": pa.Column(str, [
            pa.Check.str_matches(r"^\d{4}-\d{1,2}-[a-z0-9_]+$"),
            pa.Check(lambda s: ~s.duplicated(), name="record_id_is_unique"),
        ]),
        "season": pa.Column(int, pa.Check.in_range(1950, THIS_YEAR), coerce=True),
        "round": pa.Column(int, pa.Check.in_range(1, 30), coerce=True),
        "race_date": pa.Column("datetime64[ns]",
                               pa.Check(lambda s: s <= pd.Timestamp.today(), name="date_not_in_future"),
                               coerce=True),

        # required text, null not allowed (nullable=False is the default)
        "race_name": pa.Column(str),
        "driver_id": pa.Column(str),
        "constructor_id": pa.Column(str),
        "constructor_name": pa.Column(str),
        "status": pa.Column(str),

        # numbers must be in a realistic range
        "grid": pa.Column(int, pa.Check.in_range(0, 30), coerce=True), # 0 means pit lane start
        "position": pa.Column(int, pa.Check.in_range(1, 30), coerce=True),
        "points": pa.Column(float, pa.Check.in_range(0, 26), coerce=True), # max is 25 + 1 for fastest lap
        "laps": pa.Column(int, pa.Check.in_range(0, 100), coerce=True),

        # either a number or one of the letters the API uses (R = retired, D = disqualified ...)
        "position_text": pa.Column(str, pa.Check.str_matches(r"^(\d{1,2}|R|D|E|W|F|N)$")),

        # these can be empty (retired cars have no time), but if filled they must make sense
        "finish_time_ms": pa.Column(float, pa.Check.gt(0), coerce=True, nullable=True),
        "fastest_lap_rank": pa.Column(float, pa.Check.in_range(1, 30), coerce=True, nullable=True),
        "avg_speed_kph": pa.Column(float, pa.Check.in_range(100, 300), coerce=True, nullable=True),
    },
    checks=[
        # rules that need two columns at once
        pa.Check(lambda df: df["race_date"].dt.year == df["season"], name="date_year_equals_season"),
        pa.Check(lambda df: (df["points"] == 0) | (df["position"] <= 10), name="points_only_for_top_10"),
    ],
)


def split_valid_invalid(df):
# "Returns (valid rows, invalid rows with an 'errors' column).
    df = df.reset_index(drop=True)
    try:
        schema.validate(df, lazy=True) # lazy = collect all errors, not only the first one
        return df, df.iloc[0:0].assign(errors="")
    except SchemaErrors as err:
        fails = err.failure_cases.dropna(subset=["index"])

    fails = fails.astype({"index": int})
    # two-column rules are reported once per row, not once per column
    row_rules = [c.name for c in schema.checks]
    fails["msg"] = fails["column"].astype(str) + ": " + fails["check"].astype(str)
    fails.loc[fails["check"].isin(row_rules), "msg"] = "row: " + fails["check"].astype(str)
    errors = fails.groupby("index")["msg"].agg(lambda m: "; ".join(sorted(set(m))))

    invalid = df.loc[errors.index].assign(errors=errors)
    valid = df.drop(index=errors.index)
    return valid, invalid


def make_test_rows(good_row):
# "Copy a real row 4 times and break each copy in a different way.
    tests = pd.DataFrame([good_row] * 4).reset_index(drop=True)
    tests["record_id"] = ["2024-1-test_a", "2024-1-test_b", "2024-1-test_c", "2024-1-test_c"]
    tests.loc[0, ["position", "points"]] = ["45", "-3"] # out of range
    tests.loc[1, ["race_date", "driver_id"]] = ["2024-02-31", None] # impossible date, missing field
    tests.loc[2, "position_text"] = "WIN" # not an allowed category
    tests.loc[3, "grid"] = "first" # wrong type, also a repeated id
    tests["what_we_broke"] = ["position and points out of range", "bad date and missing driver_id",
                              "unknown position_text", "grid is text and record_id repeats"]
    return tests


def main():
    if not config.CONSUMED_CSV.exists():
        raise SystemExit("data/consumed_data.csv not found, run the consumer first")
    received = pd.read_csv(config.CONSUMED_CSV, dtype=str)

    valid, invalid = split_valid_invalid(received)
    valid.to_csv(config.VALID_CSV, index=False)
    invalid.to_csv(config.INVALID_CSV, index=False)
    print(f"Validation: {len(valid)} valid, {len(invalid)} invalid out of {len(received)}")

    # validation test with the broken rows
    tests = make_test_rows(received.iloc[0])
    _, caught = split_valid_invalid(tests.drop(columns="what_we_broke"))
    caught = caught.join(tests["what_we_broke"])
    caught[["record_id", "what_we_broke", "errors"]].to_csv(config.TEST_RESULTS_CSV, index=False)

    print(f"Test rows caught: {len(caught)} of {len(tests)}")
    for _, row in caught.iterrows():
        print(f"  {row['record_id']} ({row['what_we_broke']}) -> {row['errors']}")

    save_counts("validate", received=len(received), valid=len(valid), invalid=len(invalid),
                test_rows=len(tests), test_rows_caught=len(caught))
    if len(caught) != len(tests):
        raise SystemExit("Schema missed some of the broken test rows")


if __name__ == "__main__":
    main()
