"""
consumer_to_mongo.py
---------------------
Reads messages from the 'order-events' Kafka topic and writes each one
into a MongoDB Atlas collection, so Atlas Charts can build a live dashboard
on top of it.

Before running:
1. Create a free MongoDB Atlas cluster (see setup guide).
2. Replace MONGO_URI below with your own connection string.
3. Run producer.py in one terminal, then this script in another.
"""

import json
import certifi
from kafka import KafkaConsumer
from pymongo import MongoClient

TOPIC_NAME = "order-events"
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Replace with your actual Atlas connection string (from "Connect" > "Drivers")
MONGO_URI = "mongodb+srv://<username>:<password>@cluster0.6tnw3yn.mongodb.net/?appName=Cluster0"
DB_NAME = "sda_assignment3"
COLLECTION_NAME = "order_events"

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    group_id="mongo-writer-group"
)

print(f"Listening on '{TOPIC_NAME}' and writing to MongoDB Atlas ({DB_NAME}.{COLLECTION_NAME}) ...")
print("Press Ctrl+C to stop.\n")

for message in consumer:
    record = message.value
    result = collection.insert_one(record)
    print(f"Inserted into MongoDB: {record}  (_id={result.inserted_id})")
