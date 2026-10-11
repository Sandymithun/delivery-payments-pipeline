"""
Stream order-lifecycle events into Kafka.

    python -m streaming.producer --rate 20          # 20 orders/second, forever
    python -m streaming.producer --orders 500       # stop after 500 orders

Each order emits a short sequence of events, keyed by order_id so every event
for an order lands on the same partition (per-order ordering is preserved):

    order_placed -> payment_captured | payment_failed -> ... -> order_delivered | order_cancelled
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

TOPIC = os.getenv("KAFKA_TOPIC", "order-events")


def order_events(rng: random.Random) -> list[dict]:
    order_id = f"ord_live_{uuid.uuid4().hex[:12]}"
    base = {
        "order_id": order_id,
        "customer_id": f"cus_{rng.randint(1, 5000):05d}",
        "merchant_id": f"mer_{rng.randint(1, 300):04d}",
    }
    amount = round(rng.uniform(8, 80), 2)
    events = [{"event_type": "order_placed", "amount": amount}]
    if rng.random() < 0.06:
        events.append({"event_type": "payment_failed", "amount": amount})
    if rng.random() < 0.07:
        events.append({"event_type": "order_cancelled"})
    else:
        events.append({"event_type": "payment_captured", "amount": amount,
                       "method": rng.choice(["card", "apple_pay", "google_pay", "paypal"])})
        events.append({"event_type": "order_delivered", "delivery_minutes": rng.randint(15, 75)})
    return [{**base, **e, "event_id": uuid.uuid4().hex} for e in events]


def main() -> None:
    from kafka import KafkaProducer

    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=10, help="orders per second")
    p.add_argument("--orders", type=int, default=0, help="stop after N orders (0 = run forever)")
    a = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",          # wait for the broker to persist the write
        linger_ms=20,        # small batching window
        retries=5,
    )
    rng, sent = random.Random(), 0
    try:
        while not a.orders or sent < a.orders:
            for event in order_events(rng):
                event["event_ts"] = datetime.now(timezone.utc).isoformat()
                producer.send(TOPIC, key=event["order_id"], value=event)
            sent += 1
            if sent % 100 == 0:
                print(f"produced {sent} orders", flush=True)
            time.sleep(1 / a.rate)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
