"""
Load one day's landed files from object storage into the warehouse RAW schema.

    python -m ingestion.load_warehouse --date 2026-10-09

WAREHOUSE=snowflake  ->  COPY INTO from an external stage on the S3/Azure bucket
WAREHOUSE=duckdb     ->  local stand-in for Snowflake; pulls the object and
                         inserts it with DuckDB (same table shapes, same SQL in dbt)

Every load is idempotent: rows for the batch date are deleted, then reloaded,
so Airflow retries and backfills never double count.
"""
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from ingestion.config import ENTITIES, RAW_COLUMNS, Settings, snowflake_connect

META_COLUMNS = ["_batch_date", "_source_file", "_loaded_at"]


# --------------------------------------------------------------------------- #
# Snowflake
# --------------------------------------------------------------------------- #
def snowflake_copy_sql(entity: str, run_date: str, s: Settings) -> list[str]:
    cols = RAW_COLUMNS[entity]
    table = f"RAW.{entity.upper()}"
    positional = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
    target_cols = ", ".join(c.upper() for c in cols + META_COLUMNS)
    return [
        f"DELETE FROM {table} WHERE _BATCH_DATE = '{run_date}'",
        f"""
        COPY INTO {table} ({target_cols})
        FROM (
            SELECT {positional}, TO_DATE('{run_date}'), METADATA$FILENAME, CURRENT_TIMESTAMP()
            FROM @{s.snowflake_stage}/{s.raw_prefix}/{entity}/dt={run_date}/
        )
        FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
        FORCE = TRUE
        ON_ERROR = ABORT_STATEMENT
        """,
        f"SELECT COUNT(*) FROM {table} WHERE _BATCH_DATE = '{run_date}'",
    ]


def load_snowflake(run_date: str, s: Settings) -> dict[str, int]:
    counts = {}
    with snowflake_connect() as conn, conn.cursor() as cur:
        for entity in ENTITIES:
            *statements, count_sql = snowflake_copy_sql(entity, run_date, s)
            for sql in statements:
                cur.execute(sql)
            counts[entity] = cur.execute(count_sql).fetchone()[0]
    return counts


# --------------------------------------------------------------------------- #
# DuckDB (local)
# --------------------------------------------------------------------------- #
def _fetch(entity: str, run_date: str, s: Settings, tmp: Path) -> tuple[Path, str]:
    """Return a local path for the landed object, downloading it if needed."""
    key = s.object_key(entity, run_date)
    if s.storage_backend == "local":
        path = s.local_file(entity, run_date)
        return path, str(path)
    dest = tmp / f"{entity}.csv"
    if s.storage_backend == "s3":
        import boto3

        boto3.client("s3", endpoint_url=s.s3_endpoint_url).download_file(s.s3_bucket, key, str(dest))
        return dest, f"s3://{s.s3_bucket}/{key}"
    if s.storage_backend == "azure":
        from azure.storage.blob import BlobServiceClient

        blob = BlobServiceClient.from_connection_string(s.azure_connection_string) \
            .get_blob_client(s.azure_container, key)
        dest.write_bytes(blob.download_blob().readall())
        return dest, f"azure://{s.azure_container}/{key}"
    raise ValueError(f"unknown STORAGE_BACKEND={s.storage_backend}")


def load_duckdb(run_date: str, s: Settings) -> dict[str, int]:
    import duckdb

    s.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    counts = {}
    with duckdb.connect(str(s.duckdb_path)) as con, tempfile.TemporaryDirectory() as tmp:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        for entity in ENTITIES:
            cols = RAW_COLUMNS[entity]
            ddl_cols = ", ".join(f"{c} VARCHAR" for c in cols)
            con.execute(
                f"CREATE TABLE IF NOT EXISTS raw.{entity} ("
                f"{ddl_cols}, _batch_date DATE, _source_file VARCHAR, _loaded_at TIMESTAMP)"
            )
            path, source_uri = _fetch(entity, run_date, s, Path(tmp))
            con.execute("BEGIN")
            con.execute(f"DELETE FROM raw.{entity} WHERE _batch_date = ?", [run_date])
            con.execute(
                f"""
                INSERT INTO raw.{entity}
                SELECT {", ".join(cols)}, CAST(? AS DATE), ?, now()
                FROM read_csv(?, header = true, all_varchar = true)
                """,
                [run_date, source_uri, str(path)],
            )
            con.execute("COMMIT")
            counts[entity] = con.execute(
                f"SELECT count(*) FROM raw.{entity} WHERE _batch_date = ?", [run_date]
            ).fetchone()[0]
    return counts


def load(run_date: str, s: Settings | None = None) -> dict[str, int]:
    s = s or Settings()
    counts = load_snowflake(run_date, s) if s.warehouse == "snowflake" else load_duckdb(run_date, s)
    empty = [e for e, n in counts.items() if n == 0]
    if empty:
        raise RuntimeError(f"no rows loaded for {empty} on {run_date}")
    return counts


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", required=True)
    for entity, n in load(p.parse_args().date).items():
        print(f"raw.{entity:<10} {n:>8,} rows")


if __name__ == "__main__":
    main()
