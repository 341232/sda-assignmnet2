"""
generate_data.py
-----------------
Generates sample data for the 'order-events' stream, matching the
Customer Mobile App data source identified in Assignment 1
(fields: customer_id, session_id, order_id, dark_store_id, cart_value, order_status).

Output: order_events.json  (a JSON array of event records)
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("en_IN")  # Indian locale fits the Quick Commerce use case
Faker.seed(42)
random.seed(42)

NUM_RECORDS = 120
ORDER_STATUSES = ["placed", "packed", "picked_up", "out_for_delivery", "delivered", "cancelled"]
DARK_STORES = [f"DS-{i:03d}" for i in range(1, 11)]  # 10 dark stores

records = []
start_time = datetime.now() - timedelta(hours=2)

for i in range(NUM_RECORDS):
    event_time = start_time + timedelta(seconds=i * random.randint(5, 20))
    record = {
        "customer_id": f"CUST-{fake.random_int(min=1000, max=9999)}",
        "session_id": str(uuid.uuid4())[:8],
        "order_id": f"ORD-{100000 + i}",
        "dark_store_id": random.choice(DARK_STORES),
        "cart_value": round(random.uniform(99.0, 2499.0), 2),
        "order_status": random.choices(
            ORDER_STATUSES, weights=[30, 15, 15, 15, 20, 5]
        )[0],
        "timestamp": event_time.isoformat()
    }
    records.append(record)

with open("order_events.json", "w") as f:
    json.dump(records, f, indent=2)

print(f"Generated {len(records)} records -> order_events.json")
print("Sample record:")
print(json.dumps(records[0], indent=2))
