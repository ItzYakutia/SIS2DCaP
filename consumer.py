"""Step 2b. Kafka consumer

Reads the batch the producer just sent, turns each JSON message back into a dict
and saves everything to data/consumed_data.csv

When does it stop?
  1. it gets the {"end_of_stream": true} message from the producer, or
  2. nothing arrives for 15 seconds (in case the marker got lost)

Old messages from earlier runs are skipped because we start reading at the offset
the producer wrote into data/counts.json

Run: python -m src.consumer (after the producer)
"""
import json

import pandas as pd
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable

from src import config
from src.counts import read_counts, save_counts


def main():
    produced = read_counts().get("produce")
    if not produced or produced["first_offset"] is None:
        raise SystemExit("No producer run found, run the producer first")
    start = produced["first_offset"]

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=config.KAFKA_SERVER,
            enable_auto_commit=False,
            consumer_timeout_ms=config.IDLE_TIMEOUT_MS,
        )
    except NoBrokersAvailable:
        raise SystemExit(f"Cannot reach Kafka at {config.KAFKA_SERVER}. Is the broker running?")

    part = TopicPartition(config.TOPIC, 0)
    consumer.assign([part])
    consumer.seek(part, start)

    records, bad = [], 0
    got_end_marker = False
    try:
        for msg in consumer:
            try:
                data = json.loads(msg.value.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                bad += 1
                print(f"  skipped broken message at offset {msg.offset}")
                continue
            if data.get("end_of_stream"):
                got_end_marker = True
                break
            records.append(data)
    except KafkaError as err:
        raise SystemExit(f"Kafka error while reading: {err}")
    finally:
        consumer.close()

    pd.DataFrame(records).to_csv(config.CONSUMED_CSV, index=False)
    save_counts("consume", consumed=len(records), broken=bad, stopped_by_end_marker=got_end_marker)

    reason = "end marker" if got_end_marker else "idle timeout"
    print(f"Consumer: {len(records)} messages consumed, {bad} broken (stopped by {reason})")
    if len(records) != produced["published"]:
        print(f"  warning: producer published {produced['published']} but we got {len(records)}")


if __name__ == "__main__":
    main()
