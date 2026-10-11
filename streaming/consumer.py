"""
Consume order events from Kafka and land them in the lake as micro-batches.

    python -m streaming.consumer --batch-size 500 --flush-seconds 30

* Writes newline-delimited JSON to  raw/order_events/dt=YYYY-MM-DD/part-<ts>-<n>.jsonl
  in the same bucket the batch pipeline uses (or data/raw locally).
* Offsets are committed only *after* a batch is safely written, so a crash means
  re-delivery, never loss (at-least-once). Downstream dedupes on event_id.
* Prints a rolling count of events per type so you can watch it work.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone

from ingestion.config import Settings

TOPIC = os.getenv("KAFKA_TOPIC", "order-events")


def write_batch(events: list[dict], s: Settings, seq: int) -> str:
    now = datetime.now(timezone.utc)
    key = f"{s.raw_prefix}/order_events/dt={now:%Y-%m-%d}/part-{now:%H%M%S}-{seq:05d}.jsonl"
    body = "\n".join(json.dumps(e) for e in events).encode()

    if s.storage_backend == "s3":
        import boto3

        boto3.client("s3", endpoint_url=s.s3_endpoint_url).put_object(Bucket=s.s3_bucket, Key=key, Body=body)
        return f"s3://{s.s3_bucket}/{key}"

    path = s.local_raw_dir.parent / key  # data/raw/order_events/...
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return str(path)


def main() -> None:
    from kafka import KafkaConsumer

    p = argparse.ArgumentParser()
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--flush-seconds", type=float, default=30)
    a = p.parse_args()

    s = Settings()
    if s.storage_backend == "s3":
        from ingestion.upload_to_storage import ensure_bucket
        ensure_bucket(s)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
        group_id=os.getenv("KAFKA_GROUP", "lake-writer"),
        enable_auto_commit=False,          # we commit after the write succeeds
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode()),
    )

    buffer: list[dict] = []
    totals: Counter = Counter()
    last_flush, seq = time.monotonic(), 0
    print(f"consuming {TOPIC} ...", flush=True)
    while True:
        for records in consumer.poll(timeout_ms=1000).values():
            for record in records:
                buffer.append(record.value)
                totals[record.value["event_type"]] += 1

        due = time.monotonic() - last_flush >= a.flush_seconds
        if buffer and (len(buffer) >= a.batch_size or due):
            seq += 1
            uri = write_batch(buffer, s, seq)
            consumer.commit()
            print(f"wrote {len(buffer):>5} events -> {uri} | totals {dict(totals)}", flush=True)
            buffer, last_flush = [], time.monotonic()


if __name__ == "__main__":
    main()
