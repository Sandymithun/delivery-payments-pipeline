"""Fast unit tests for the Python parts (dbt tests cover the SQL)."""
from __future__ import annotations

import csv
import random
from datetime import date
from decimal import Decimal

import pytest

from generator.generate_data import generate
from ingestion.config import RAW_COLUMNS, Settings
from ingestion.load_warehouse import load_duckdb, snowflake_copy_sql
from streaming.producer import order_events

DAY = date(2026, 10, 5)


def read(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


@pytest.fixture
def batch(tmp_path):
    return generate(DAY, tmp_path / "raw", n_orders=300, seed=7), tmp_path


def test_generator_is_deterministic(tmp_path):
    a = generate(DAY, tmp_path / "a", n_orders=50, seed=1)
    b = generate(DAY, tmp_path / "b", n_orders=50, seed=1)
    for entity in a:
        assert a[entity].read_bytes() == b[entity].read_bytes()


def test_columns_match_raw_contract(batch):
    paths, _ = batch
    for entity, path in paths.items():
        assert list(read(path)[0].keys()) == RAW_COLUMNS[entity]


def test_order_totals_reconcile(batch):
    paths, _ = batch
    for o in read(paths["orders"]):
        parts = sum(Decimal(o[c]) for c in ("subtotal", "delivery_fee", "tip"))
        assert parts == Decimal(o["total_amount"])


def test_orders_only_reference_existing_entities(batch):
    paths, _ = batch
    customers = {r["customer_id"] for r in read(paths["customers"])}
    merchants = {r["merchant_id"] for r in read(paths["merchants"])}
    for o in read(paths["orders"]):
        assert o["customer_id"] in customers and o["merchant_id"] in merchants


def test_raw_payments_contain_the_quality_issues_dbt_must_fix(tmp_path):
    paths = generate(DAY, tmp_path, n_orders=2000, seed=42)
    payments = read(paths["payments"])
    ids = [p["payment_id"] for p in payments]
    assert len(ids) > len(set(ids)), "expected webhook-retry duplicates"
    assert {"captured", "failed", "voided", "refunded"} <= {p["status"] for p in payments}


def test_duckdb_load_is_idempotent(batch, monkeypatch):
    paths, tmp = batch
    s = Settings(local_raw_dir=tmp / "raw", storage_backend="local", duckdb_path=tmp / "wh.duckdb")
    first = load_duckdb(DAY.isoformat(), s)
    second = load_duckdb(DAY.isoformat(), s)  # Airflow retry / rerun
    assert first == second
    assert first["orders"] == 300


def test_snowflake_copy_is_scoped_to_the_partition():
    delete, copy, _ = snowflake_copy_sql("orders", "2026-10-05", Settings())
    assert "_BATCH_DATE = '2026-10-05'" in delete
    assert "/raw/orders/dt=2026-10-05/" in copy and "METADATA$FILENAME" in copy


def test_stream_events_follow_the_order_lifecycle():
    for _ in range(200):
        events = order_events(random.Random())
        types = [e["event_type"] for e in events]
        assert types[0] == "order_placed"
        assert types[-1] in {"order_delivered", "order_cancelled"}
        assert len({e["order_id"] for e in events}) == 1  # same key -> same partition
