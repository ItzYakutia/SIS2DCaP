"""
Runs the whole pipeline, or only some steps.

python main.py all steps, database rebuilt from scratch
python main.py collect produce only these steps
python main.py report just print the counts check
"""
import subprocess
import sys

from src import config
from src.counts import read_counts

STEPS = ["collect", "produce", "consume", "validate", "clean", "load", "analyze"]
SCRIPTS = {
    "collect": "src.api_client",
    "produce": "src.producer",
    "consume": "src.consumer",
    "validate": "src.validator",
    "clean": "src.cleaner",
    "load": "src.loader",
    "analyze": "src.analysis",
}


def run_step(step, fresh_db):
    print(f"\n----- {step} -----", flush=True)
    cmd = [sys.executable, "-m", SCRIPTS[step]]
    if step == "load" and fresh_db:
        cmd.append("--fresh")
    if subprocess.run(cmd, cwd=config.ROOT).returncode != 0:
        raise SystemExit(f"Step '{step}' failed, stopping here")


def report():
    c = read_counts()
    try:
        numbers = [
            ("records collected from API", c["collect"]["records"]),
            ("messages published to Kafka", c["produce"]["published"]),
            ("messages consumed from Kafka", c["consume"]["consumed"]),
            ("valid records", c["validate"]["valid"]),
            ("invalid records", c["validate"]["invalid"]),
            ("records after cleaning", c["clean"]["rows_out"]),
            ("records stored in SQLite", c["load"]["stored"]),
        ]
    except KeyError as missing:
        raise SystemExit(f"No numbers for step {missing} yet, run the full pipeline first")

    print("\n----- pipeline check -----")
    for label, value in numbers:
        print(f"  {label:<32}{value:>6}")
    v, ld = c["validate"], c["load"]
    print(f"  {'broken test rows caught':<32}{v['test_rows_caught']:>6} of {v['test_rows']}")
    print(f"  {'second load added':<32}{ld['second_load_added']:>6} rows (idempotent: {ld['idempotent']})")

    # explain any gaps between the steps
    collected, published = c["collect"]["records"], c["produce"]["published"]
    consumed, valid = c["consume"]["consumed"], v["valid"]
    cleaned, stored = c["clean"]["rows_out"], ld["stored"]
    notes = []
    if published != collected:
        notes.append(f"{collected - published} records were not delivered to Kafka")
    if consumed != published:
        notes.append(f"consumer got {consumed} of {published} messages (stopped by timeout?)")
    if v["invalid"]:
        notes.append(f"{v['invalid']} invalid records kept aside in data/invalid_records.csv")
    if cleaned != valid:
        notes.append(f"{valid - cleaned} duplicates removed while cleaning")
    if stored != cleaned:
        notes.append("database has a different number of rows, try: python main.py load --fresh")
    print("\n  " + ("\n  ".join(notes) if notes else "all counts match, nothing was lost between steps"))


def main(args):
    fresh = "--fresh" in args
    steps = [a for a in args if not a.startswith("--")]
    if not steps or steps == ["all"]:
        steps, fresh = STEPS + ["report"], True

    wrong = [s for s in steps if s not in STEPS and s != "report"]
    if wrong:
        raise SystemExit(f"Unknown step {wrong}. Use: {', '.join(STEPS)}, report")

    for step in steps:
        if step == "report":
            report()
        else:
            run_step(step, fresh)


if __name__ == "__main__":
    main(sys.argv[1:])
