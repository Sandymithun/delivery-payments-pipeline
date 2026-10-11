"""Shared settings for ingestion, read from environment variables (see .env.example)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Column order for each raw entity. Raw tables are all VARCHAR on purpose:
# the landing layer stores exactly what the source sent, and dbt staging casts.
RAW_COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "first_name", "last_name", "email", "city", "signup_date"],
    "merchants": ["merchant_id", "merchant_name", "category", "city", "commission_rate",
                  "onboarded_date", "updated_at"],
    "orders": ["order_id", "customer_id", "merchant_id", "order_ts", "status", "items_count",
               "subtotal", "delivery_fee", "tip", "total_amount", "currency", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "method", "amount", "currency", "status"],
}
ENTITIES = list(RAW_COLUMNS)


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value if value not in ("", None) else default


@dataclass(frozen=True)
class Settings:
    # where raw files are generated before upload
    local_raw_dir: Path = field(default_factory=lambda: Path(_env("LOCAL_RAW_DIR", str(PROJECT_ROOT / "data" / "raw"))))

    # ---- object storage --------------------------------------------------
    storage_backend: str = field(default_factory=lambda: _env("STORAGE_BACKEND", "s3"))  # s3 | azure | local
    raw_prefix: str = field(default_factory=lambda: _env("RAW_PREFIX", "raw"))
    s3_bucket: str = field(default_factory=lambda: _env("S3_BUCKET", "delivery-lake"))
    s3_endpoint_url: str | None = field(default_factory=lambda: _env("S3_ENDPOINT_URL"))  # MinIO locally, unset for AWS
    azure_connection_string: str | None = field(default_factory=lambda: _env("AZURE_STORAGE_CONNECTION_STRING"))
    azure_container: str = field(default_factory=lambda: _env("AZURE_CONTAINER", "delivery-lake"))

    # ---- warehouse ---------------------------------------------------------
    warehouse: str = field(default_factory=lambda: _env("WAREHOUSE", "duckdb"))  # duckdb | snowflake
    duckdb_path: Path = field(default_factory=lambda: Path(_env("DUCKDB_PATH", str(PROJECT_ROOT / "warehouse" / "delivery.duckdb"))))
    snowflake_stage: str = field(default_factory=lambda: _env("SNOWFLAKE_STAGE", "RAW.LAKE_STAGE"))

    def object_key(self, entity: str, run_date: str) -> str:
        return f"{self.raw_prefix}/{entity}/dt={run_date}/{entity}.csv"

    def local_file(self, entity: str, run_date: str) -> Path:
        return self.local_raw_dir / entity / f"dt={run_date}" / f"{entity}.csv"


def snowflake_connect():
    """Open a Snowflake connection from SNOWFLAKE_* env vars."""
    import snowflake.connector  # imported lazily so DuckDB-only users don't need it

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=_env("SNOWFLAKE_ROLE", "TRANSFORMER"),
        warehouse=_env("SNOWFLAKE_WAREHOUSE", "TRANSFORMING"),
        database=_env("SNOWFLAKE_DATABASE", "DELIVERY"),
        schema="RAW",
    )
