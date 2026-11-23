# Data dictionary

Source: Jolpica F1 API, `GET /ergast/f1/{season}/results.json`. One row is one driver's result in one race.

## Columns from the API (raw_data.csv, valid_records.csv)

Everything comes in as text. The type column shows what it becomes after cleaning.

| Column | Type | Kind | Meaning |
|---|---|---|---|
| record_id | text | id | `season-round-driver_id`, for example `2024-5-hamilton`. Unique |
| season | int | numeric | year of the championship |
| round | int | numeric | race number in that season |
| race_name | text | categorical | e.g. Bahrain Grand Prix |
| race_date | date | date/time | day of the race |
| circuit_name | text | categorical | track name |
| country | text | categorical | country of the track |
| driver_id | text | categorical | short driver code from the API, e.g. max_verstappen |
| given_name, family_name | text | categorical | driver's first and last name |
| driver_nationality | text | categorical | e.g. Dutch, British |
| constructor_id | text | categorical | team code, e.g. red_bull |
| constructor_name | text | categorical | team name |
| grid | int | numeric | starting position, 0 = started from the pit lane |
| position | int | numeric | final position |
| position_text | text | categorical | position as a number, or R (retired), D (disqualified), W, F, N, E |
| points | float | numeric | points scored in the race |
| laps | int | numeric | laps completed |
| status | text | categorical | Finished, Lapped, Engine, Collision ... |
| finish_time_ms | float | numeric | race time in milliseconds, empty if lapped or retired |
| fastest_lap_rank | int | numeric | rank of the driver's best lap in that race, can be empty |
| avg_speed_kph | float | numeric | average speed on the best lap, can be empty |

## Columns added in cleaning (cleaned_data.csv, table race_results)

| Column | How it is made |
|---|---|
| driver_name | given_name + " " + family_name |
| status_group | Finished / Lapped / Disqualified / Retired (everything else) |
| finish_time_s | finish_time_ms / 1000 |
| positions_gained | grid - position, empty if grid = 0 |
| is_podium | 1 if position <= 3, else 0 |
| is_dnf | 1 if status_group is Retired or Disqualified |
| points_share | driver's points / all points given in that race |

given_name, family_name, position_text and finish_time_ms are not stored in the database
(driver_name, position and finish_time_s replace them).

## SQLite table race_results

Primary key `record_id`, unique `(season, round, driver_id)`. Columns: record_id, season, round, race_name,
race_date (text YYYY-MM-DD), circuit_name, country, driver_id, driver_name, driver_nationality,
constructor_id, constructor_name, grid, position, points, laps, status, status_group, finish_time_s,
fastest_lap_rank, avg_speed_kph, positions_gained, is_podium, is_dnf, points_share.
