# SIS2: Data Streaming and Validation Pipeline with Apache Kafka and Pandera
## F1 race results through Kafka

Data Collection & Preparation, SIS 2

We take Formula 1 race results for 2022, 2023 and 2024 from a free public API, push them through a real
Apache Kafka broker, check them with Pandera, clean them with pandas/NumPy, save them to SQLite and make
a few charts. 3 seasons give 1359 rows (one row = one driver in one race).

```
Jolpica API -> raw_data.csv -> Kafka producer -> topic -> Kafka consumer -> Pandera -> cleaning -> SQLite -> charts
```
## Setup

```bash
python -m venv .venv
source .venv/bin/activate # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Kafka

Start the broker (single node, KRaft mode, port 9092):

```bash
docker compose up -d
```

Give it 10 to 20 seconds before running the producer. Stop it later with `docker compose down`.

If you have Kafka installed with Homebrew instead, `brew services start kafka` works too, it also listens on
localhost:9092.

Topic: `f1-race-results`, 1 partition, replication factor 1. The producer creates it on its first run, so you
don't need to do anything. If you want to create it by hand:

```bash
docker exec sis2-kafka /opt/kafka/bin/kafka-topics.sh --create --topic f1-race-results \
  --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```

The broker address, topic name and consumer timeout are in `src/config.py`.

## API

- API: Jolpica F1 (the continuation of the old Ergast API). No key, no account, nothing secret to configure.
- Endpoint: `GET https://api.jolpi.ca/ergast/f1/{season}/results.json`
- Parameters: `limit=100` (the maximum page size) and `offset` for paging
- Docs: https://github.com/jolpica/jolpica-f1

`src/api_client.py` asks for page after page until it reaches the `total` the API reports. If the connection
fails, or the API answers 429 or 5xx, it waits and tries again (4 tries). Other codes like 404 stop the script
with a message. Every page is saved unchanged in `data/raw/` before anything else happens, so the original
data is always there.

Seasons are set in `src/config.py` (`SEASONS`).

## Running it

Everything at once (the database is rebuilt from zero):

```bash
python main.py
```

Or step by step, in this order:

```bash
python -m src.api_client       # 1. API -> data/raw/*.json, data/raw_data.csv
python -m src.producer         # 2. raw_data.csv -> Kafka topic
python -m src.consumer         # 3. Kafka topic -> data/consumed_data.csv
python -m src.validator        # 4. Pandera -> valid_records.csv / invalid_records.csv
python -m src.cleaner          # 5. cleaning -> cleaned_data.csv, data/tables/
python -m src.loader --fresh   # 6. SQLite -> data/output.db (loads twice)
python -m src.analysis         # 7. EDA -> figures/*.png, data/eda_summary.json
python main.py report          # count check over all steps
```

`main.py` can also run a few steps, e.g. `python main.py produce consume`.

### How producer and consumer talk

- The producer turns each row into JSON (`json.dumps`) and sends it to partition 0 of the topic, with
  `acks="all"` so Kafka confirms every message. It counts confirmed and failed messages.
- After the last row it sends `{"end_of_stream": true, "count": N}`.
- It writes the offset of its first message into `data/counts.json`. The consumer starts reading exactly
  there, so messages from older runs that are still in the topic are not read again.
- The consumer turns each message back into a dict (`json.loads`). It stops when it sees the end message, or
  after 15 seconds with no new messages. Broken messages are counted and skipped.
- If the broker is not running, both scripts stop with a clear message instead of a long traceback.

### Validation

`src/validator.py` has one Pandera schema with rules for 17 columns and 2 rules that look at two columns:
types (with coercion from text), required fields, ranges (grid 0 to 30, position 1 to 30, points 0 to 26,
laps, speed), allowed values for `position_text`, valid date not in the future, `record_id` format and
uniqueness, race year equals season, and points only for the top 10.

Invalid rows are not deleted, they are moved to `data/invalid_records.csv` with the error text. Our real data
had 0 invalid rows, so the script also makes 4 broken test rows (ids `2024-1-test_...`) and shows that all 4
are caught. The result is in `data/validation_test_results.csv`. Test rows are checked separately and never
mixed into the real data.

### Database

One table, `race_results`. `record_id` is the primary key and `(season, round, driver_id)` is unique. Rows are
inserted with `INSERT OR IGNORE`, so a row that already exists is skipped.

The loader loads the same CSV twice and checks that the second load adds 0 rows and leaves the table exactly
the same. You can run `python -m src.loader` (without `--fresh`) any time to repeat this.

Check queries are in `sql/verify_queries.sql`. The loader runs them, or you can run them yourself:

```bash
sqlite3 data/output.db < sql/verify_queries.sql
```

## What you should see

`python main.py report` at the end prints:

```
  records collected from API        1359
  messages published to Kafka       1359
  messages consumed from Kafka      1359
  valid records                     1359
  invalid records                      0
  records after cleaning            1359
  records stored in SQLite          1359
  broken test rows caught              4 of 4
  second load added                    0 rows (idempotent: True)
```

If the numbers differ, the report says why (failed sends, consumer timeout, invalid rows, duplicates).
The 1359 can change a little if the API adds or corrects data.

Files after a full run:

| File | What it is |
|---|---|
| data/raw/*.json | API pages, untouched |
| data/raw_data.csv | flat table from the API |
| data/consumed_data.csv | what came out of Kafka |
| data/valid_records.csv, invalid_records.csv | Pandera result |
| data/validation_test_results.csv | the 4 broken test rows and their errors |
| data/cleaned_data.csv, data/cleaning_log.txt | cleaned data and notes on each step |
| data/tables/*.csv | groupby, pivot_table and melt results |
| data/output.db | SQLite database |
| data/eda_summary.json | numbers from the analysis |
| data/counts.json | counts from every step |
| figures/*.png | 4 charts |

## Checking that it worked

1. The consumer line says `stopped by end marker` and consumed = published.
2. The validator line says `Test rows caught: 4 of 4`.
3. The loader says `load 2 added 0 rows` and `Table unchanged after second load: True`.
4. `sqlite3 data/output.db "SELECT COUNT(*) FROM race_results"` gives the same number as the cleaned rows.
5. `figures/` has 4 png files.
6. `python tests/test_steps.py` prints `all tests passed` (no Kafka or internet needed).

## Charts

1. `01_grid_vs_finish.png` - finishing position by starting position
2. `02_team_points_heatmap.png` - team points per season
3. `03_dnf_rate_by_team.png` - how often each team did not finish
4. `04_positions_gained.png` - places gained or lost in a race, by season

## Files

```
main.py - runs the steps in order + count report
src/config.py - paths and settings
src/counts.py - saves step counts to data/counts.json
src/api_client.py - step 1, API
src/producer.py - step 2, Kafka producer
src/consumer.py - step 3, Kafka consumer
src/validator.py - step 4, Pandera
src/cleaner.py - step 5, pandas / NumPy
src/loader.py - step 6, SQLite
src/analysis.py - step 7, EDA and charts
sql/verify_queries.sql
docs/data_dictionary.md
tests/test_steps.py
docker-compose.yml
report/SIS2_Report.pdf
```
