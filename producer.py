"""
producer.py
-----------
Reads order_events.json row by row, serialises each record as JSON,
and publishes it to the 'order-events' Kafka topic — the topic identified
for the Customer Mobile App source in Assignment 1's architecture.

Partition key: order_id (keeps every event for a given order in sequence,
matching the partitioning strategy chosen in Assignment 1).
"""

import json
import time
from kafka import KafkaProducer

TOPIC_NAME = "order-events"
BOOTSTRAP_SERVERS = ["localhost:9092"]
DATA_FILE = "order_events.json"
SEND_INTERVAL_SECONDS = 1  # 1 message per second, matching Assignment 1's stated frequency

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all"  # wait for broker acknowledgement before considering the write complete
)


def load_records(path):
    with open(path, "r") as f:
        return json.load(f)


def main():
    records = load_records(DATA_FILE)
    print(f"Loaded {len(records)} records from {DATA_FILE}")
    print(f"Publishing to topic '{TOPIC_NAME}' at {BOOTSTRAP_SERVERS} ...\n")

    for record in records:
        key = record["order_id"]
        future = producer.send(TOPIC_NAME, key=key, value=record)
        try:
            metadata = future.get(timeout=10)
            print(
                f"Sent: {json.dumps(record)}  "
                f"-> partition={metadata.partition}, offset={metadata.offset}"
            )
        except Exception as e:
            print(f"Failed to send record {record.get('order_id')}: {e}")

        time.sleep(SEND_INTERVAL_SECONDS)

    producer.flush()
    producer.close()
    print("\nAll records sent. Producer closed.")


if __name__ == "__main__":
    main()
