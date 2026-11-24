"""Step 6. EDA on the data stored in SQLite, plus 4 charts in figures/

Questions we want to answer:
  Q1. Does starting near the front mean finishing near the front?
  Q2. Which teams scored the most, and how did it change by season?
  Q3. Which teams retire most often (reliability)?
  Q4. How many places do drivers usually gain or lose during a race?

Run: python -m src.analysis
"""
import json
import sqlite3

import matplotlib
matplotlib.use("Agg") # save to files, no window
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from src import config
from src.counts import save_counts

sns.set_theme(style="whitegrid")
findings = {}


def overview(df):
    print(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns\n")
    print("Types:")
    print(df.dtypes.to_string(), "\n")
    print("Numbers:")
    print(df[["grid", "position", "points", "laps", "finish_time_s", "avg_speed_kph",
              "positions_gained"]].describe().round(2).to_string(), "\n")
    print("Most common values:")
    for col in ["status_group", "constructor_name", "country"]:
        print(f"  {col}: {df[col].value_counts().head(5).to_dict()}")

    per_season = df.groupby("season").agg(races=("round", "nunique"), entries=("record_id", "count"),
                                          avg_points=("points", "mean"), dnf_rate=("is_dnf", "mean"))
    print("\nBy season:")
    print(per_season.round(3).to_string(), "\n")
    findings["per_season"] = per_season.round(3).reset_index().to_dict("records")


def chart_grid_vs_finish(df):
    finished = df[df["status_group"].isin(["Finished", "Lapped"])].copy()
    finished["start"] = pd.cut(finished["grid"].replace(0, 20), bins=[0, 3, 6, 10, 15, 30],
                               labels=["1-3", "4-6", "7-10", "11-15", "16+"])
    # Spearman = normal correlation of the ranks (saves us installing scipy)
    corr = finished["grid"].rank().corr(finished["position"].rank())

    fig, ax = plt.subplots(figsize=(9, 5.5))
    sns.boxplot(data=finished, x="start", y="position", color="#6aa3d5", ax=ax)
    ax.invert_yaxis()
    ax.set_title("Finishing position by starting position (cars that finished)")
    ax.set_xlabel("Starting grid position")
    ax.set_ylabel("Finishing position (1 = win)")
    ax.text(0.02, 0.04, f"Spearman correlation = {corr:.2f}", transform=ax.transAxes)
    fig.tight_layout()
    fig.savefig(config.FIGURES / "01_grid_vs_finish.png", dpi=150)
    plt.close(fig)
    findings["q1_spearman"] = round(corr, 3)
    findings["q1_median_finish"] = finished.groupby("start", observed=True)["position"].median().to_dict()


def chart_team_points(df):
    pivot = df.pivot_table(index="constructor_name", columns="season", values="points",
                           aggfunc="sum", fill_value=0)
    pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).index]

    fig, ax = plt.subplots(figsize=(8, 7))
    sns.heatmap(pivot, annot=True, fmt=".0f", cmap="Oranges", cbar_kws={"label": "Points"}, ax=ax)
    ax.set_title("Team points per season (Grand Prix results only)")
    ax.set_xlabel("Season")
    ax.set_ylabel("Team")
    fig.tight_layout()
    fig.savefig(config.FIGURES / "02_team_points_heatmap.png", dpi=150)
    plt.close(fig)
    findings["q2_best_team_each_season"] = pivot.idxmax().to_dict()
    findings["q2_top3_total"] = pivot.sum(axis=1).head(3).to_dict()


def chart_dnf_rate(df):
    teams = df.groupby("constructor_name").agg(entries=("record_id", "count"), dnf=("is_dnf", "mean"))
    teams = teams[teams["entries"] >= 40].sort_values("dnf", ascending=False)
    avg = df["is_dnf"].mean() * 100

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(teams.index, teams["dnf"] * 100, color="#d97b66")
    ax.invert_yaxis()
    ax.axvline(avg, color="black", linestyle="--", label=f"average of all teams ({avg:.1f}%)")
    ax.set_title("How often each team did not finish (retired or disqualified)")
    ax.set_xlabel("DNF rate, % of starts")
    ax.set_ylabel("Team")
    ax.legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(config.FIGURES / "03_dnf_rate_by_team.png", dpi=150)
    plt.close(fig)
    findings["q3_avg_dnf_pct"] = round(avg, 2)
    findings["q3_highest_dnf"] = teams["dnf"].head(3).round(3).to_dict()
    findings["q3_lowest_dnf"] = teams["dnf"].tail(3).round(3).to_dict()


def chart_positions_gained(df):
    gained = df.dropna(subset=["positions_gained"])

    fig, ax = plt.subplots(figsize=(9, 5.5))
    sns.histplot(data=gained, x="positions_gained", hue="season", discrete=True,
                 multiple="dodge", shrink=0.8, palette="Set2", ax=ax)
    ax.axvline(0, color="black", linewidth=1)
    ax.set_title("Places gained or lost from start to finish")
    ax.set_xlabel("Places gained (grid minus finish), below 0 = lost places")
    ax.set_ylabel("Number of results")
    fig.tight_layout()
    fig.savefig(config.FIGURES / "04_positions_gained.png", dpi=150)
    plt.close(fig)
    stats = gained.groupby("season")["positions_gained"].agg(["mean", "median", "std"]).round(2)
    findings["q4_gained_by_season"] = stats.reset_index().to_dict("records")


def main():
    if not config.DB_FILE.exists():
        raise SystemExit("data/output.db not found, run the loader first")
    config.FIGURES.mkdir(exist_ok=True)

    with sqlite3.connect(config.DB_FILE) as conn:
        df = pd.read_sql_query("SELECT * FROM race_results", conn, parse_dates=["race_date"])

    overview(df)
    chart_grid_vs_finish(df)
    chart_team_points(df)
    chart_dnf_rate(df)
    chart_positions_gained(df)

    (config.DATA / "eda_summary.json").write_text(json.dumps(findings, indent=2, default=str))
    save_counts("analyze", rows=len(df), charts=4)
    print("Findings:")
    print(json.dumps({k: v for k, v in findings.items() if k != "per_season"}, indent=2, default=str))
    print("\n4 charts saved in figures/")


if __name__ == "__main__":
    main()
