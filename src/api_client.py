"""Step 1. Download F1 race results from the Jolpica API

Request: GET https://api.jolpi.ca/ergast/f1/<season>/results.json?limit=100&offset=<n>
One page has at most 100 rows, so for every season we move the offset until we reach "total"
Each page is saved untouched in data/raw/ and the flat table goes to data/raw_data.csv

Run: python -m src.api_client
"""
import json
import time

import pandas as pd
import requests

from src import config
from src.counts import save_counts


def get_page(season, offset):
    url = f"{config.API_URL}/{season}/results.json"
    params = {"limit": config.PAGE_SIZE, "offset": offset}

    for attempt in range(1, config.MAX_TRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
        except requests.RequestException as err:
            print(f"  connection problem: {err} (try {attempt})")
            time.sleep(2 * attempt)
            continue

        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                print(f"  answer is not valid JSON (try {attempt})")
        elif resp.status_code == 429 or resp.status_code >= 500:
            # too many requests or server problem, worth waiting and trying again
            print(f"  HTTP {resp.status_code} (try {attempt})")
        else:
            # 400, 404 and so on will not fix themselves
            raise RuntimeError(f"API returned HTTP {resp.status_code} for {resp.url}")
        time.sleep(2 * attempt)

    raise RuntimeError(f"gave up on season {season}, offset {offset}")


def page_to_rows(page):
    """The API nests results inside races. We want one row per driver per race."""
    rows = []
    for race in page["MRData"]["RaceTable"]["Races"]:
        for res in race["Results"]:
            driver = res["Driver"]
            team = res["Constructor"]
            fastest = res.get("FastestLap", {})
            rows.append({
                "record_id": f"{race['season']}-{race['round']}-{driver['driverId']}",
                "season": race["season"],
                "round": race["round"],
                "race_name": race["raceName"],
                "race_date": race["date"],
                "circuit_name": race["Circuit"]["circuitName"],
                "country": race["Circuit"]["Location"]["country"],
                "driver_id": driver["driverId"],
                "given_name": driver["givenName"],
                "family_name": driver["familyName"],
                "driver_nationality": driver.get("nationality"),
                "constructor_id": team["constructorId"],
                "constructor_name": team["name"],
                "grid": res.get("grid"),
                "position": res.get("position"),
                "position_text": res.get("positionText"),
                "points": res.get("points"),
                "laps": res.get("laps"),
                "status": res.get("status"),
                "finish_time_ms": res.get("Time", {}).get("millis"),
                "fastest_lap_rank": fastest.get("rank"),
                "avg_speed_kph": fastest.get("AverageSpeed", {}).get("speed"),
            })
    return rows


def collect():
    config.RAW_JSON_DIR.mkdir(parents=True, exist_ok=True)
    all_rows = []
    requests_done = 0

    for season in config.SEASONS:
        offset, total = 0, 1
        while offset < total:
            page = get_page(season, offset)
            requests_done += 1
            total = int(page["MRData"]["total"])

            # keep the original answer before we touch anything
            raw_file = config.RAW_JSON_DIR / f"{season}_offset{offset:04d}.json"
            raw_file.write_text(json.dumps(page))

            rows = page_to_rows(page)
            all_rows += rows
            print(f"  {season}: rows {offset}-{offset + len(rows)} of {total}")
            offset += config.PAGE_SIZE
            time.sleep(config.PAUSE)

    df = pd.DataFrame(all_rows)
    df.to_csv(config.RAW_CSV, index=False)
    save_counts("collect", records=len(df), requests=requests_done, seasons=config.SEASONS)
    print(f"Collected {len(df)} records in {requests_done} requests, saved to data/raw_data.csv")


if __name__ == "__main__":
    try:
        collect()
    except RuntimeError as err:
        raise SystemExit(f"Collection stopped: {err}")
