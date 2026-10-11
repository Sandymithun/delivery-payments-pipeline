"""
Synthetic food-delivery / payments data generator.

Produces one *daily batch* of raw CSV files, laid out the way a real
source-system export would land in a data lake:

    <out>/customers/dt=YYYY-MM-DD/customers.csv   (full snapshot)
    <out>/merchants/dt=YYYY-MM-DD/merchants.csv   (full snapshot)
    <out>/orders/dt=YYYY-MM-DD/orders.csv         (that day's orders)
    <out>/payments/dt=YYYY-MM-DD/payments.csv     (that day's payment events)

Output is deterministic for a given (--seed, --date) so reruns and backfills
are idempotent.

The raw data deliberately contains a few realistic quality issues that the
dbt staging layer must handle:
  * duplicate payment events (gateway webhook retries, same payment_id)
  * failed payment attempts followed by a successful retry
  * refunds recorded as separate negative-amount payment rows
  * e-mail addresses with stray whitespace / mixed case
  * merchants whose commission rate changes over time (drives an SCD2 dimension)
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import random
from datetime import date, datetime, timedelta
from pathlib import Path

N_CUSTOMERS = 5_000
N_MERCHANTS = 300
EPOCH = date(2026, 1, 1)  # first day any customer/merchant can exist

CITIES = ["Dublin", "Cork", "Galway", "Limerick", "Waterford", "Belfast"]
CATEGORIES = ["pizza", "burger", "sushi", "indian", "chinese", "healthy", "grocery", "dessert"]
PAYMENT_METHODS = ["card", "card", "card", "apple_pay", "google_pay", "paypal"]
FIRST = ["Aoife", "Conor", "Siobhan", "Liam", "Niamh", "Sean", "Ciara", "Darragh", "Emma",
         "Jack", "Sophie", "Oisin", "Grace", "Cian", "Chloe", "Ryan", "Saoirse", "Adam"]
LAST = ["Murphy", "Kelly", "O'Brien", "Walsh", "Byrne", "Ryan", "O'Connor", "Doyle",
        "McCarthy", "Gallagher", "Kennedy", "Lynch", "Quinn", "Moore", "Nolan"]
MERCHANT_WORDS = ["Golden", "Urban", "Little", "Spice", "Green", "Harbour", "Rocket",
                  "Corner", "Wild", "Lucky", "Fresh", "Copper"]


def _rng(*parts: object) -> random.Random:
    """Deterministic RNG from arbitrary key parts."""
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()
    return random.Random(int(digest[:16], 16))


# --------------------------------------------------------------------------- #
# Master data (stable across runs; filtered to what exists on the run date)
# --------------------------------------------------------------------------- #
def build_customers(seed: int) -> list[dict]:
    r = _rng(seed, "customers")
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        first, last = r.choice(FIRST), r.choice(LAST)
        email = f"{first}.{last}{i}@example.com".replace("'", "").lower()
        if r.random() < 0.05:  # dirty e-mail in source system
            email = f"  {email.upper()} "
        rows.append({
            "customer_id": f"cus_{i:05d}",
            "first_name": first,
            "last_name": last,
            "email": email,
            "city": r.choice(CITIES),
            "signup_date": (EPOCH + timedelta(days=r.randint(0, 330))).isoformat(),
        })
    return rows


def build_merchants(seed: int, run_date: date) -> list[dict]:
    r = _rng(seed, "merchants")
    rows = []
    for i in range(1, N_MERCHANTS + 1):
        onboarded = EPOCH + timedelta(days=r.randint(0, 300))
        base_rate = round(r.choice([0.12, 0.15, 0.18, 0.20, 0.25]), 2)
        # ~20% of merchants renegotiate commission on a fixed date
        change_day = onboarded + timedelta(days=r.randint(20, 200)) if r.random() < 0.2 else None
        rate, updated_at = base_rate, onboarded
        if change_day and run_date >= change_day:
            rate, updated_at = round(base_rate + 0.03, 2), change_day
        rows.append({
            "merchant_id": f"mer_{i:04d}",
            "merchant_name": f"{r.choice(MERCHANT_WORDS)} {r.choice(CATEGORIES).title()} {i}",
            "category": r.choice(CATEGORIES),
            "city": r.choice(CITIES),
            "commission_rate": f"{rate:.2f}",
            "onboarded_date": onboarded.isoformat(),
            "updated_at": f"{updated_at.isoformat()} 00:00:00",
        })
    return rows


# --------------------------------------------------------------------------- #
# Transactional data for one day
# --------------------------------------------------------------------------- #
def build_orders_and_payments(seed: int, run_date: date, n_orders: int,
                              customers: list[dict], merchants: list[dict]):
    r = _rng(seed, "orders", run_date.isoformat())
    day_start = datetime.combine(run_date, datetime.min.time())
    orders, payments = [], []
    pay_seq = 0

    def next_payment_id() -> str:
        nonlocal pay_seq
        pay_seq += 1
        return f"pay_{run_date:%Y%m%d}_{pay_seq:07d}"

    for i in range(1, n_orders + 1):
        cust = r.choice(customers)
        merch = r.choice(merchants)
        # lunch and dinner peaks
        hour = r.choices(range(24), weights=[1, 1, 1, 1, 1, 1, 2, 3, 3, 3, 4, 8,
                                             10, 8, 4, 3, 4, 7, 11, 12, 10, 6, 3, 2])[0]
        order_ts = day_start + timedelta(hours=hour, minutes=r.randint(0, 59), seconds=r.randint(0, 59))
        items = r.randint(1, 6)
        subtotal = round(sum(r.uniform(4, 18) for _ in range(items)), 2)
        delivery_fee = r.choice([0.0, 1.99, 2.49, 2.99, 3.99])
        tip = round(r.choice([0, 0, 0, 1, 2, 3, 5]) * 1.0, 2)
        total = round(subtotal + delivery_fee + tip, 2)
        status = r.choices(["delivered", "cancelled", "refunded"], weights=[90, 7, 3])[0]
        delivery_minutes = r.randint(15, 75) if status != "cancelled" else ""

        order_id = f"ord_{run_date:%Y%m%d}_{i:06d}"
        orders.append({
            "order_id": order_id,
            "customer_id": cust["customer_id"],
            "merchant_id": merch["merchant_id"],
            "order_ts": order_ts.isoformat(sep=" "),
            "status": status,
            "items_count": items,
            "subtotal": f"{subtotal:.2f}",
            "delivery_fee": f"{delivery_fee:.2f}",
            "tip": f"{tip:.2f}",
            "total_amount": f"{total:.2f}",
            "currency": "EUR",
            "delivery_minutes": delivery_minutes,
        })

        method = r.choice(PAYMENT_METHODS)
        t = order_ts + timedelta(seconds=r.randint(2, 30))

        def pay(status_: str, amount: float, ts: datetime, pid: str | None = None) -> dict:
            return {
                "payment_id": pid or next_payment_id(),
                "order_id": order_id,
                "payment_ts": ts.isoformat(sep=" "),
                "method": method,
                "amount": f"{amount:.2f}",
                "currency": "EUR",
                "status": status_,
            }

        if status == "cancelled":
            # cancelled before capture: authorisation voided
            payments.append(pay("voided", total, t))
            continue

        if r.random() < 0.06:  # declined first, then retried successfully
            payments.append(pay("failed", total, t))
            t += timedelta(seconds=r.randint(20, 120))
        captured = pay("captured", total, t)
        payments.append(captured)
        if r.random() < 0.03:  # webhook retry -> exact duplicate event
            payments.append(dict(captured))
        if status == "refunded":
            payments.append(pay("refunded", -total, t + timedelta(hours=r.randint(1, 5))))

    return orders, payments


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def generate(run_date: date, out_dir: Path, n_orders: int = 2_000, seed: int = 42) -> dict[str, Path]:
    customers = [c for c in build_customers(seed) if date.fromisoformat(c["signup_date"]) <= run_date]
    merchants = [m for m in build_merchants(seed, run_date) if date.fromisoformat(m["onboarded_date"]) <= run_date]
    orders, payments = build_orders_and_payments(seed, run_date, n_orders, customers, merchants)

    dt = f"dt={run_date.isoformat()}"
    paths = {
        "customers": out_dir / "customers" / dt / "customers.csv",
        "merchants": out_dir / "merchants" / dt / "merchants.csv",
        "orders": out_dir / "orders" / dt / "orders.csv",
        "payments": out_dir / "payments" / dt / "payments.csv",
    }
    for name, rows in [("customers", customers), ("merchants", merchants),
                       ("orders", orders), ("payments", payments)]:
        write_csv(rows, paths[name])
    return paths


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", default=date.today().isoformat(), help="business date YYYY-MM-DD")
    p.add_argument("--orders", type=int, default=2_000, help="orders to generate for the day")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data/raw", help="output root directory")
    a = p.parse_args()

    paths = generate(date.fromisoformat(a.date), Path(a.out), a.orders, a.seed)
    for name, path in paths.items():
        n = sum(1 for _ in path.open()) - 1
        print(f"{name:<10} {n:>8,} rows -> {path}")


if __name__ == "__main__":
    main()
