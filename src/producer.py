"""Step 2a. Kafka producer

Reads data/raw_data.csv and sends every row to the topic as a JSON message
After the last row it sends one more message {"end_of_stream": true, "count": N},
so the consumer knows when the batch is over

Run: python -m src.producer   (the broker must be running, see README)
"""
import json

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError

from src import config
from src.counts import save_counts


def create_topic():
    admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_SERVER)
    try:
        admin.create_topics([NewTopic(config.TOPIC, num_partitions=1, replication_factor=1)])
        print(f"Topic '{config.TOPIC}' created")
    except TopicAlreadyExistsError:
        print(f"Topic '{config.TOPIC}' already exists, using it")
    finally:
        admin.close()


def main():
    if not config.RAW_CSV.exists():
        raise SystemExit("data/raw_data.csv not found, run the api_client first")

    df = pd.read_csv(config.RAW_CSV, dtype=str)
    # NaN is not valid JSON, so empty cells become null
    records = df.astype(object).where(df.notna(), None).to_dict("records")

    try:
        create_topic()
        producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_SERVER,
            value_serializer=lambda obj: json.dumps(obj).encode("utf-8"),
            acks="all",
            retries=3,
        )
    except NoBrokersAvailable:
        raise SystemExit(f"Cannot reach Kafka at {config.KAFKA_SERVER}. Is the broker running?")

    # everything goes to partition 0, so the consumer can read it in order
    futures = [producer.send(config.TOPIC, value=rec, partition=0) for rec in records]
    producer.flush()

    sent, failed, first_offset = 0, 0, None
    for fut in futures:
        try:
            meta = fut.get(timeout=10)
            sent += 1
            if first_offset is None:
                first_offset = meta.offset
        except KafkaError as err:
            failed += 1
            print(f"  message not delivered: {err}")

    # end marker, sent and confirmed separately
    try:
        producer.send(config.TOPIC, value={"end_of_stream": True, "count": sent}, partition=0).get(timeout=10)
    except KafkaError as err:
        print(f"  end marker not delivered ({err}), consumer will stop on idle timeout")
    producer.close()

    # first_offset tells the consumer where this batch starts in the topic
    save_counts("produce", published=sent, failed=failed, first_offset=first_offset, topic=config.TOPIC)
    print(f"Producer: {sent} messages published, {failed} failed (topic {config.TOPIC}, starting at offset {first_offset})")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
