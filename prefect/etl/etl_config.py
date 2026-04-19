# config.py
import os
from dotenv import load_dotenv
import clickhouse_connect

load_dotenv("/opt/prefect/.env")  # Adjust path if needed

class Config:
    # MinIO (S3-compatible object storage)
    MINIO_URL = "minio:9000"
    MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
    MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123456")
    BUCKET = "dlh-landing"  # Bucket for staging parquet files

    # Source PostgreSQL (api-data) – where raw data originates
    API_PG_URL = os.getenv(
        "API_PG_URL",
        f"postgresql://{os.getenv('API_POSTGRES_USER')}:{os.getenv('API_POSTGRES_PASSWORD')}@api-data:5432/{os.getenv('API_POSTGRES_DB')}"
    )

    # Target PostgreSQL (data warehouse) – where final and mart tables live
    PG_URL = os.getenv(
        "PG_URL",
        f"postgresql://{os.getenv('DATA_POSTGRES_USER')}:{os.getenv('DATA_POSTGRES_PASSWORD')}@postgres-data:5432/{os.getenv('DATA_POSTGRES_DB')}"
    )

    # Schema names
    SCHEMA_STAGE = "trnx_stage"
    SCHEMA_FINAL = "trnx_final"
    SCHEMA_SOURCE = "public"  # Schema in api-data

    # ClickHouse client (for analytical queries)
    CH_CLIENT = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )

# Singleton instance for easy import
config = Config()