"""Step 4. Clean and transform the valid records with pandas and NumPy

Each step prints what it did and why. The same notes are saved to data/cleaning_log.txt
so we can use them in the report

Run: python -m src.cleaner
"""
import numpy as np
import pandas as pd

from src import config
from src.counts import save_counts

# the API is not consistent with country names, we want full names everywhere
COUNTRY_NAMES = {"UK": "United Kingdom", "USA": "United States", "UAE": "United Arab Emirates"}

FINAL_COLUMNS = [
    "record_id", "season", "round", "race_name", "race_date", "circuit_name", "country",
    "driver_id", "driver_name", "driver_nationality", "constructor_id", "constructor_name",
    "grid", "position", "points", "laps", "status", "status_group", "finish_time_s",
    "fastest_lap_rank", "avg_speed_kph", "positions_gained", "is_podium", "is_dnf", "points_share",
]

log = []


def note(text):
    print("  " + text)
    log.append(text)


def clean(df):
    df = df.copy()

    # 1. missing values
    missing = df.isna().sum()
    note(f"missing values per column: {missing[missing > 0].to_dict()}")
    note("finish time / fastest lap / speed are empty for lapped or retired cars. That is normal, "
         "so we keep them empty (NaN) instead of inventing a value")
    df["driver_nationality"] = df["driver_nationality"].fillna("unknown")

    # 2. duplicates
    before = len(df)
    df = df.drop_duplicates(subset="record_id")
    note(f"removed {before - len(df)} duplicate rows (a driver has only one result per race, "
         "a copy would count the points twice)")

    # 3. types (everything came through Kafka as text)
    for col in ["season", "round", "grid", "position", "laps"]:
        df[col] = df[col].astype(int)
    for col in ["points", "finish_time_ms", "avg_speed_kph", "fastest_lap_rank"]:
        df[col] = pd.to_numeric(df[col])
    df["race_date"] = pd.to_datetime(df["race_date"])
    note("converted numbers to int/float and race_date to datetime, otherwise we cannot do math or sort by date")

    # 4. text
    text_cols = ["race_name", "circuit_name", "country", "given_name", "family_name",
                 "constructor_name", "driver_nationality", "status"]
    for col in text_cols:
        df[col] = df[col].str.strip().str.replace(r"\s+", " ", regex=True)
    for col in ["driver_id", "constructor_id"]:
        df[col] = df[col].str.strip().str.lower()
    df["country"] = df["country"].replace(COUNTRY_NAMES)
    df["driver_name"] = df["given_name"] + " " + df["family_name"]

    # status has many values (Finished, Lapped, Engine, Collision, Gearbox ...), we group them into 4
    df["status_group"] = "Retired"
    df.loc[df["status"] == "Finished", "status_group"] = "Finished"
    df.loc[df["status"].str.contains(r"Lapped|\+\d+ Lap"), "status_group"] = "Lapped"
    df.loc[df["status"] == "Disqualified", "status_group"] = "Disqualified"
    note(f"trimmed spaces, lowercased ids, fixed country names, grouped status: "
         f"{df['status_group'].value_counts().to_dict()}")

    # 5. NumPy columns
    grid = df["grid"].to_numpy()
    pos = df["position"].to_numpy()
    df["positions_gained"] = np.where(grid > 0, grid - pos, np.nan)  # grid 0 = pit lane, no fair number
    df["is_podium"] = (pos <= 3).astype(int)
    df["is_dnf"] = np.isin(df["status_group"], ["Retired", "Disqualified"]).astype(int)
    df["finish_time_s"] = np.round(df["finish_time_ms"].to_numpy() / 1000, 3)
    race_total = df.groupby(["season", "round"])["points"].transform("sum").to_numpy()
    df["points_share"] = np.round(np.where(race_total > 0, df["points"] / race_total, 0), 4)
    note("added positions_gained, is_podium, is_dnf, finish_time_s, points_share with NumPy")

    # 6. filter with several conditions: started 10th or worse but finished in the top 5
    comebacks = df[(df["grid"] >= 10) & (df["position"] <= 5) & (df["status_group"] != "Retired")]
    note(f"comeback drives (grid >= 10, finish top 5, not retired): {len(comebacks)}")

    return df[FINAL_COLUMNS].reset_index(drop=True)


def make_tables(df):
    """GroupBy summaries and reshaping, saved to data/tables/."""
    config.TABLES_DIR.mkdir(parents=True, exist_ok=True)

    team_season = df.groupby(["season", "constructor_name"]).agg(
        points=("points", "sum"),
        wins=("position", lambda p: (p == 1).sum()),
        podiums=("is_podium", "sum"),
        dnf_rate=("is_dnf", "mean"),
    ).reset_index()

    drivers = df.groupby("driver_name").agg(
        races=("record_id", "count"),
        points=("points", "sum"),
        avg_gained=("positions_gained", "mean"),
    ).sort_values("points", ascending=False).reset_index()

    # long -> wide: one row per team, one column per season
    pivot = df.pivot_table(index="constructor_name", columns="season", values="points",
                           aggfunc="sum", fill_value=0)
    # wide -> long again with melt
    melted = pivot.reset_index().melt(id_vars="constructor_name", var_name="season", value_name="points")

    team_season.to_csv(config.TABLES_DIR / "team_season.csv", index=False)
    drivers.to_csv(config.TABLES_DIR / "driver_summary.csv", index=False)
    pivot.to_csv(config.TABLES_DIR / "points_pivot.csv")
    melted.to_csv(config.TABLES_DIR / "points_long.csv", index=False)
    note(f"groupby tables: team_season ({len(team_season)} rows), driver_summary ({len(drivers)} rows); "
         f"pivot_table {pivot.shape} and melt back to {len(melted)} rows")


def main():
    if not config.VALID_CSV.exists():
        raise SystemExit("data/valid_records.csv not found, run the validator first")
    valid = pd.read_csv(config.VALID_CSV, dtype=str)

    print("Cleaning:")
    cleaned = clean(valid)
    make_tables(cleaned)
    cleaned.to_csv(config.CLEANED_CSV, index=False)
    (config.DATA / "cleaning_log.txt").write_text("\n".join(log) + "\n")

    save_counts("clean", rows_in=len(valid), rows_out=len(cleaned))
    print(f"Cleaning done: {len(valid)} rows in, {len(cleaned)} rows out")


if __name__ == "__main__":
    main()
