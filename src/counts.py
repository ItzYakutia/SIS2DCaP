# Every step writes its numbers into data/counts.json so main.py can compare them at the end
import json

from src.config import COUNTS_FILE

def read_counts():
    if COUNTS_FILE.exists():
        return json.loads(COUNTS_FILE.read_text())
    return {}

def save_counts(step, **numbers):
    counts = read_counts()
    counts[step] = numbers
    COUNTS_FILE.parent.mkdir(exist_ok=True)
    COUNTS_FILE.write_text(json.dumps(counts, indent=2, default=str))
