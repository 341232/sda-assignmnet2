# SDA Assignment 3 — Dashboard for Analysis of Consumed Data

This builds on Assignment 2's Kafka producer by adding a consumer that writes
streamed data into MongoDB Atlas, and an Atlas Charts dashboard for analysis.

## Files

- **producer.py** — generates and sends order events to the `order-events` Kafka topic (from Assignment 2).
- **consumer_to_mongo.py** — reads from the `order-events` Kafka topic and writes each record into MongoDB Atlas (`sda_assignment3.order_events`).
- **Order_Events_Dashboard.charts** — exported MongoDB Atlas Charts dashboard, importable directly into Atlas Charts.
- **generate_data.py** — generates the sample order event data.
- **docker-compose.yml** — spins up a local Kafka + ZooKeeper broker.

## Data Fields

Each order event contains:
- `customer_id`
- `session_id`
- `order_id`
- `dark_store_id`
- `cart_value`
- `order_status`
- `timestamp`

## How to Run

1. Start Kafka: `docker-compose up -d`
2. Install dependencies: `pip install -r requirements.txt`
3. Generate sample data: `python3 generate_data.py`
4. In `consumer_to_mongo.py`, set `MONGO_URI` to your own MongoDB Atlas connection string (not committed here for security).
5. Run the producer: `python3 producer.py`
6. In a separate terminal, run the consumer: `python3 consumer_to_mongo.py`
7. Verify data lands in Atlas under the `sda_assignment3.order_events` collection.

## Dashboard

Import `Order_Events_Dashboard.charts` into MongoDB Atlas Charts and map its data
source to your own `sda_assignment3.order_events` collection. It contains 6 charts:

1. **Total Orders** (Number) — count of all order events
2. **Average Cart Value** (Number) — mean cart value across all orders
3. **Orders by Status** (Donut) — breakdown of orders by `order_status`
4. **Average Cart Value by Dark Store** (Grouped Bar) — mean `cart_value` per `dark_store_id`
5. **Orders Over Time** (Line) — order count over `timestamp`
6. **Order Status by Dark Store** (Stacked Bar) — `order_status` counts broken down by `dark_store_id`

## Business Insight

The dashboard highlights which dark stores see higher average order values,
how order status is distributed (e.g. cancellation rate), and how order volume
trends over time — useful for spotting fulfillment issues or demand patterns
across dark stores.
