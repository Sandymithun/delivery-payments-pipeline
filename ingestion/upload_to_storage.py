"""
Land one day's raw CSV files in object storage (the "bronze"/landing zone).

    python -m ingestion.upload_to_storage --date 2026-10-09

Backends (STORAGE_BACKEND):
  s3     AWS S3, or MinIO locally when S3_ENDPOINT_URL is set   (default)
  azure  Azure Blob Storage via AZURE_STORAGE_CONNECTION_STRING
  local  no-op; files stay on disk (handy for unit tests / CI)

Keys follow Hive-style partitioning: raw/<entity>/dt=YYYY-MM-DD/<entity>.csv,
so re-uploading a date overwrites that partition and nothing else.
"""
from __future__ import annotations

import argparse

from ingestion.config import ENTITIES, Settings


def _s3_client(s: Settings):
    import boto3

    return boto3.client("s3", endpoint_url=s.s3_endpoint_url)


def ensure_bucket(s: Settings) -> None:
    if s.storage_backend == "s3":
        client = _s3_client(s)
        existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
        if s.s3_bucket not in existing:
            client.create_bucket(Bucket=s.s3_bucket)
    elif s.storage_backend == "azure":
        from azure.core.exceptions import ResourceExistsError
        from azure.storage.blob import BlobServiceClient

        svc = BlobServiceClient.from_connection_string(s.azure_connection_string)
        try:
            svc.create_container(s.azure_container)
        except ResourceExistsError:
            pass


def upload(run_date: str, s: Settings | None = None) -> list[str]:
    s = s or Settings()
    uploaded = []
    if s.storage_backend == "local":
        return [str(s.local_file(e, run_date)) for e in ENTITIES]

    ensure_bucket(s)
    for entity in ENTITIES:
        src = s.local_file(entity, run_date)
        if not src.exists():
            raise FileNotFoundError(f"{src} not found - run the generator first")
        key = s.object_key(entity, run_date)

        if s.storage_backend == "s3":
            _s3_client(s).upload_file(str(src), s.s3_bucket, key)
            uploaded.append(f"s3://{s.s3_bucket}/{key}")
        elif s.storage_backend == "azure":
            from azure.storage.blob import BlobServiceClient

            blob = BlobServiceClient.from_connection_string(s.azure_connection_string) \
                .get_blob_client(s.azure_container, key)
            with src.open("rb") as fh:
                blob.upload_blob(fh, overwrite=True)
            uploaded.append(f"azure://{s.azure_container}/{key}")
        else:
            raise ValueError(f"unknown STORAGE_BACKEND={s.storage_backend}")
    return uploaded


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", required=True)
    for uri in upload(p.parse_args().date):
        print("landed", uri)


if __name__ == "__main__":
    main()
