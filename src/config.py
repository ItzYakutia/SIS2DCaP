# Settings used by all the scripts. Change seasons or Kafka address here
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RAW_JSON_DIR = DATA / "raw" # API pages saved exactly as they came
TABLES_DIR = DATA / "tables" # groupby / pivot results
FIGURES = ROOT / "figures"
SQL_CHECKS = ROOT / "sql" / "verify_queries.sql"

# files passed from one step to the next
RAW_CSV = DATA / "raw_data.csv"
CONSUMED_CSV = DATA / "consumed_data.csv"
VALID_CSV = DATA / "valid_records.csv"
INVALID_CSV = DATA / "invalid_records.csv"
TEST_RESULTS_CSV = DATA / "validation_test_results.csv"
CLEANED_CSV = DATA / "cleaned_data.csv"
DB_FILE = DATA / "output.db"
COUNTS_FILE = DATA / "counts.json"

# Jolpica F1 API (free, no key needed)
API_URL = "https://api.jolpi.ca/ergast/f1"
SEASONS = [2022, 2023, 2024]
PAGE_SIZE = 100 # API does not allow more than 100 rows per request
MAX_TRIES = 4
PAUSE = 0.5 # seconds between requests, the API has a rate limit

# Kafka
KAFKA_SERVER = "localhost:9092"
TOPIC = "f1-race-results"
IDLE_TIMEOUT_MS = 15000 # consumer gives up if nothing arrives for 15 s
