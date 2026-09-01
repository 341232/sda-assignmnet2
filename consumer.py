"""
consumer.py (optional, for verification)
-----------------------------------------
Reads back messages from 'order-events' so you can confirm the producer
worked, independent of the producer's own terminal output.
Run this in a second terminal BEFORE or AFTER running producer.py.
Press Ctrl+C to stop.
"""

import json
from kafka import KafkaConsumer

TOPIC_NAME = "order-events"
BOOTSTRAP_SERVERS = ["localhost:9092"]

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    group_id="verification-group"
)

print(f"Listening on '{TOPIC_NAME}' ... (Ctrl+C to stop)\n")
for message in consumer:
    print(f"Received: key={message.key} value={message.value}")
