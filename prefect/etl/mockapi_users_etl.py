from prefect import task, flow
import requests
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text
import clickhouse_connect
import numpy as np
import os
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional
import psycopg2
from contextlib import contextmanager

# ====================== CONFIG ======================
load_dotenv("/opt/prefect/.env")


class Config:
    MINIO_URL = "minio:9000"
    MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
    MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123456")
    BUCKET = "users"

    # Подключение к api-data (источник)
    API_PG_URL = os.getenv(
        "API_PG_URL",
        f"postgresql://{os.getenv('API_POSTGRES_USER')}:{os.getenv('API_POSTGRES_PASSWORD')}@api-data:5432/{os.getenv('API_POSTGRES_DB')}"
    )

    # Подключение к postgres-data (целевое хранилище)
    PG_URL = os.getenv(
        "PG_URL",
        f"postgresql://{os.getenv('DATA_POSTGRES_USER')}:{os.getenv('DATA_POSTGRES_PASSWORD')}@postgres-data:5432/{os.getenv('DATA_POSTGRES_DB')}"
    )

    SCHEMA = 'trnx'
    SCHEMA_API = 'public'
    TABLE = 'users'

    CH_CLIENT = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


config = Config()


# ====================== HELPERS ======================
@contextmanager
def pg_connection():
    conn = psycopg2.connect(config.PG_URL)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def api_connection():
    conn = psycopg2.connect(config.API_PG_URL)
    try:
        yield conn
    finally:
        conn.close()


# ====================== TASKS ======================
@task(retries=3, retry_delay_seconds=10)
def extract_from_api() -> pd.DataFrame:
    """Извлечение данных о пользователях из api-data"""
    with api_connection() as conn:
        query = f"SELECT user_id, email, full_name, created_at, is_blocked FROM {config.SCHEMA_API}.{config.TABLE}"
        df = pd.read_sql(query, conn)

    print(f"✓ Извлечено {len(df)} записей из api-data.{config.SCHEMA_API}.{config.TABLE}")
    return df


@task
def load_to_minio_stage(df: pd.DataFrame) -> str:
    client = Minio(
        config.MINIO_URL,
        access_key=config.MINIO_USER,
        secret_key=config.MINIO_PASS,
        secure=False
    )

    if not client.bucket_exists(config.BUCKET):
        client.make_bucket(config.BUCKET)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    object_name = f"stage/{config.TABLE}_{timestamp}.parquet"

    df.to_parquet("/tmp/temp.parquet", index=False)
    client.fput_object(config.BUCKET, object_name, "/tmp/temp.parquet")

    print(f"✓ Данные сохранены в MinIO: {object_name}")
    return object_name


@task
def load_stage_to_pg(df: pd.DataFrame):
    df_loaded = df.copy()

    with pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.SCHEMA}_stage.{config.TABLE} RESTART IDENTITY")

            columns = df_loaded.columns.tolist()
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO {config.SCHEMA}_stage.{config.TABLE} ({', '.join(columns)}) VALUES ({placeholders})"

            cur.executemany(sql, [tuple(row) for row in df_loaded.values])
            conn.commit()

    print(f"✓ Загружено {len(df_loaded)} записей в postgres-data.{config.SCHEMA}_stage.{config.TABLE}")


@task
def switch_stage_to_final():
    engine = create_engine(config.PG_URL)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"CALL etl.switch_procedure('{config.TABLE}', '{config.SCHEMA}_final', '{config.SCHEMA}_stage', 'user_id');")
        )


@task
def load_to_clickhouse(df: pd.DataFrame):
    config.CH_CLIENT.command(f"TRUNCATE TABLE {config.SCHEMA}.{config.TABLE}")
    config.CH_CLIENT.insert_df(f"{config.SCHEMA}.{config.TABLE}", df)
    print(f"✓ Загружено {len(df)} записей в ClickHouse")


# ====================== FLOW ======================
@flow(name="Users ETL from API to Warehouse", log_prints=True)
def users_etl_flow():
    print(f"\n=== НАЧАЛО ETL: {config.TABLE} ===\n")

    df = extract_from_api()

    if df.empty:
        print("Данные пустые. Пайплайн завершён.")
        return

    load_to_minio_stage(df)
    load_stage_to_pg(df)
    switch_stage_to_final()

    engine = create_engine(config.PG_URL)
    with engine.connect() as conn:
        final_df = pd.read_sql(f"SELECT * FROM {config.SCHEMA}_final.{config.TABLE}", conn.connection)

    load_to_clickhouse(final_df)

    print(f"\n✅ ETL для {config.TABLE} завершён успешно. Загружено {len(final_df)} пользователей.\n")
