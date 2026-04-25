from prefect import task, flow
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from etl_helpers import pg_connection, load_to_clickhouse
from etl_config import config

# ====================== CONFIG ======================
TABLE_NAME = "transactions"
DAYS_BACK = 7


# ====================== TASKS ======================
@task(retries=3, retry_delay_seconds=10)
def extract_data(start_date: datetime = None, end_date: datetime = None) -> tuple[pd.DataFrame, datetime, datetime]:
    """Извлечение данных за период"""
    end_date = end_date or datetime.now()
    start_date = start_date or (end_date - timedelta(days=DAYS_BACK))

    with pg_connection("api") as conn:
        df = pd.read_sql(
            f"SELECT * FROM {config.SCHEMA_SOURCE}.{TABLE_NAME} WHERE created_at BETWEEN %(start)s AND %(end)s",
            conn,
            params={"start": start_date, "end": end_date}
        )

    print(f"✓ Извлечено {len(df)} записей за {start_date.date()} - {end_date.date()}")
    return df, start_date, end_date


@task
def save_to_minio(df: pd.DataFrame, start_date: datetime, end_date: datetime) -> str:
    """Сохранение в MinIO"""
    client = Minio(config.MINIO_URL, access_key=config.MINIO_USER, secret_key=config.MINIO_PASS, secure=False)
    if not client.bucket_exists(config.BUCKET):
        client.make_bucket(config.BUCKET)

    object_name = f"stage/{TABLE_NAME}_{start_date:%Y%m%d}_{end_date:%Y%m%d}_{datetime.now():%H%M%S}.parquet"
    df.to_parquet(f"/tmp/{TABLE_NAME}.parquet", index=False)
    client.fput_object(config.BUCKET, object_name, f"/tmp/{TABLE_NAME}.parquet")

    print(f"✓ Сохранено в MinIO: {object_name}")
    return object_name


@task
def load_stage(df: pd.DataFrame):
    """Загрузка в stage таблицу"""
    with pg_connection("warehouse") as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.SCHEMA_STAGE}.{TABLE_NAME}")

            cols = df.columns.tolist()
            sql = f"INSERT INTO {config.SCHEMA_STAGE}.{TABLE_NAME} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
            cur.executemany(sql, [tuple(row) for row in df.values])
            conn.commit()

    print(f"✓ Загружено {len(df)} записей в stage")


@task
def merge_to_final():
    """MERGE через процедуру"""
    engine = create_engine(config.PG_URL)
    with engine.begin() as conn:
        conn.execute(text("CALL etl.merge_procedure(:t, :target, :source)"),
                     {"t": TABLE_NAME, "target": config.SCHEMA_FINAL, "source": config.SCHEMA_STAGE})

    # Очищаем stage
    with pg_connection("warehouse") as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.SCHEMA_STAGE}.{TABLE_NAME}")
            conn.commit()

    print(f"✓ MERGE выполнен, stage очищен")


@task
def load_to_ch(start_date: datetime, end_date: datetime):
    """Загрузка в ClickHouse за тот же период"""
    engine = create_engine(config.PG_URL)
    with engine.connect() as conn:
        df = pd.read_sql(f"SELECT * FROM {config.SCHEMA_FINAL}.{TABLE_NAME} WHERE created_at BETWEEN %s AND %s",
                         conn.connection, params=[start_date, end_date])

    if df.empty:
        print("✓ Нет данных для ClickHouse")
        return

    # Удаляем старые данные за этот период
    config.CH_CLIENT.command(
        f"ALTER TABLE trnx.{TABLE_NAME} DELETE WHERE created_at BETWEEN '{start_date}' AND '{end_date}'")
    load_to_clickhouse(df, f"trnx.{TABLE_NAME}")
    print(f"✓ Загружено {len(df)} записей в ClickHouse")


# ====================== FLOW ======================
@flow(name="Transactions ETL", log_prints=True)
def transactions_etl_flow(start_date: datetime = None, end_date: datetime = None):
    """Основной ETL процесс"""
    print(f"\n=== ЗАГРУЗКА {TABLE_NAME.upper()} ===\n")

    df, s_date, e_date = extract_data(start_date, end_date)

    if df.empty:
        print("Нет данных. Завершение.")
        return

    save_to_minio(df, s_date, e_date)
    load_stage(df)
    merge_to_final()
    load_to_ch(s_date, e_date)

    # Статистика по дням
    df['date'] = pd.to_datetime(df['created_at']).dt.date
    print(f"\n✅ Завершено. Загружено {len(df)} транзакций за {df['date'].nunique()} дней")
    for date, count in df.groupby('date').size().items():
        print(f"   {date}: {count}")
    print()


if __name__ == "__main__":
    # За последние 7 дней
    transactions_etl_flow()

    # За произвольный период
    # transactions_etl_flow(start_date=datetime(2024, 1, 1), end_date=datetime(2024, 1, 31))