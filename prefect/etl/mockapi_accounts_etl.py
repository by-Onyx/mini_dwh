from prefect import task, flow
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text
from datetime import datetime
from etl_helpers import pg_connection, load_to_clickhouse
from etl_config import config

# ====================== CONFIG ======================
TABLE_NAME = "accounts"
PRIMARY_KEY = "account_id"


# ====================== TASKS ======================
@task(retries=3, retry_delay_seconds=10)
def extract_from_api() -> pd.DataFrame:
    """Извлечение данных из api-data"""
    with pg_connection("api") as conn:
        query = f"SELECT * FROM {config.SCHEMA_SOURCE}.{TABLE_NAME}"
        df = pd.read_sql(query, conn)

    print(f"✓ Извлечено {len(df)} записей из api-data.{config.SCHEMA_SOURCE}.{TABLE_NAME}")
    return df


@task
def load_to_minio_stage(df: pd.DataFrame) -> str:
    """Сохранение данных в MinIO в формате parquet"""
    client = Minio(
        config.MINIO_URL,
        access_key=config.MINIO_USER,
        secret_key=config.MINIO_PASS,
        secure=False
    )

    if not client.bucket_exists(config.BUCKET):
        client.make_bucket(config.BUCKET)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    object_name = f"stage/{TABLE_NAME}_{timestamp}.parquet"

    df.to_parquet(f"/tmp/{TABLE_NAME}_temp.parquet", index=False)
    client.fput_object(config.BUCKET, object_name, f"/tmp/{TABLE_NAME}_temp.parquet")

    print(f"✓ Данные сохранены в MinIO: {object_name}")
    return object_name


@task
def load_stage_to_pg(df: pd.DataFrame):
    """Загрузка данных в stage-таблицу PostgreSQL"""
    df_loaded = df.copy()

    with pg_connection("warehouse") as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.SCHEMA_STAGE}.{TABLE_NAME} RESTART IDENTITY")

            columns = df_loaded.columns.tolist()
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO {config.SCHEMA_STAGE}.{TABLE_NAME} ({', '.join(columns)}) VALUES ({placeholders})"

            cur.executemany(sql, [tuple(row) for row in df_loaded.values])
            conn.commit()

    print(f"✓ Загружено {len(df_loaded)} записей в {config.SCHEMA_STAGE}.{TABLE_NAME}")


@task
def switch_stage_to_final():
    """Переключение stage таблицы в final"""
    engine = create_engine(config.PG_URL)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"CALL etl.switch_procedure('{TABLE_NAME}', '{config.SCHEMA_FINAL}', '{config.SCHEMA_STAGE}', '{PRIMARY_KEY}');"
            )
        )
    print(f"✓ Переключена таблица {TABLE_NAME} из stage в final")


@task
def load_to_clickhouse_final():
    """Загрузка финальных данных в ClickHouse"""
    engine = create_engine(config.PG_URL)
    with engine.connect() as conn:
        final_df = pd.read_sql(f"SELECT * FROM {config.SCHEMA_FINAL}.{TABLE_NAME}", conn.connection)

    if not final_df.empty:
        load_to_clickhouse(final_df, f"trnx.{TABLE_NAME}")
        print(f"✓ Загружено {len(final_df)} записей в ClickHouse.trnx.{TABLE_NAME}")


# ====================== FLOW ======================
@flow(name=f"{TABLE_NAME.capitalize()} ETL from API to Warehouse", log_prints=True)
def accounts_etl_flow():
    print(f"\n=== НАЧАЛО ETL: {TABLE_NAME} ===\n")

    df = extract_from_api()

    if df.empty:
        print("Данные пустые. Пайплайн завершён.")
        return

    load_to_minio_stage(df)
    load_stage_to_pg(df)
    switch_stage_to_final()
    load_to_clickhouse_final()

    print(f"\n✅ ETL для {TABLE_NAME} завершён успешно.\n")


if __name__ == "__main__":
    accounts_etl_flow()