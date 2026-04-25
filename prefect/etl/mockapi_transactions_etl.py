from prefect import task, flow
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Optional
from etl_helpers import pg_connection, load_to_clickhouse
from etl_config import config

TABLE = "transactions"
DAYS = 7


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка DataFrame от NaT и NaN значений"""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].where(pd.notnull(df[col]), None)
    return df


@task
def extract(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """Извлечение данных за период"""
    end = end_date or datetime.now()
    start = start_date or (end - timedelta(days=DAYS))

    with pg_connection("api") as conn:
        df = pd.read_sql(
            f"SELECT * FROM {config.SCHEMA_SOURCE}.{TABLE} WHERE created_at BETWEEN %s AND %s",
            conn,
            params=[start, end]
        )

    print(f"✓ Извлечено {len(df)} транзакций за {start.date()} - {end.date()}")
    return clean_df(df), start, end


@task
def to_minio(df: pd.DataFrame, start_date: datetime, end_date: datetime):
    """Сохранение в MinIO"""
    client = Minio(
        config.MINIO_URL,
        access_key=config.MINIO_USER,
        secret_key=config.MINIO_PASS,
        secure=False
    )
    if not client.bucket_exists(config.BUCKET):
        client.make_bucket(config.BUCKET)

    name = f"stage/{TABLE}_{start_date:%Y%m%d}_{end_date:%Y%m%d}_{datetime.now():%H%M%S}.parquet"
    df.to_parquet("/tmp/t.parquet", index=False)
    client.fput_object(config.BUCKET, name, "/tmp/t.parquet")
    print(f"✓ Сохранено в MinIO: {name}")
    return name


@task
def to_stage(df: pd.DataFrame):
    """Загрузка в stage таблицу"""
    with pg_connection("warehouse") as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.SCHEMA_STAGE}.{TABLE}")

            cols = df.columns.tolist()
            placeholders = ', '.join(['%s'] * len(cols))
            sql = f"INSERT INTO {config.SCHEMA_STAGE}.{TABLE} ({', '.join(cols)}) VALUES ({placeholders})"

            # Подготовка данных
            data = []
            for row in df.to_numpy():
                converted_row = []
                for val in row:
                    if pd.isna(val):
                        converted_row.append(None)
                    elif isinstance(val, pd.Timestamp):
                        converted_row.append(val.to_pydatetime())
                    else:
                        converted_row.append(val)
                data.append(tuple(converted_row))

            cur.executemany(sql, data)
            conn.commit()

    print(f"✓ Загружено {len(df)} записей в stage")


@task
def merge_to_final():
    """MERGE данных в final таблицу"""
    engine = create_engine(config.PG_URL)

    # Получаем список колонок
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = :schema AND table_name = :table
                ORDER BY ordinal_position
            """),
            {"schema": config.SCHEMA_FINAL, "table": TABLE}
        )
        columns = [row[0] for row in result]

    pk_col = 'transaction_id'
    update_cols = [c for c in columns if c != pk_col]
    update_set = ', '.join([f"{c} = s.{c}" for c in update_cols])
    insert_cols = ', '.join(columns)
    insert_vals = ', '.join([f"s.{c}" for c in columns])

    merge_sql = text(f"""
        MERGE INTO {config.SCHEMA_FINAL}.{TABLE} t
        USING {config.SCHEMA_STAGE}.{TABLE} s
        ON t.{pk_col} = s.{pk_col}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    with engine.begin() as conn:
        conn.execute(merge_sql)

    # Очищаем stage
    with pg_connection("warehouse") as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.SCHEMA_STAGE}.{TABLE}")
            conn.commit()

    print(f"✓ MERGE выполнен, stage очищен")


@task
def to_clickhouse(start_date: datetime, end_date: datetime):
    """Загрузка в ClickHouse"""
    engine = create_engine(config.PG_URL)
    with engine.connect() as conn:
        df = pd.read_sql(
            f"SELECT * FROM {config.SCHEMA_FINAL}.{TABLE} WHERE created_at BETWEEN %s AND %s",
            conn.connection,
            params=[start_date, end_date]
        )

    if not df.empty:
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        config.CH_CLIENT.command(
            f"ALTER TABLE trnx.{TABLE} DELETE WHERE created_at BETWEEN '{start_str}' AND '{end_str}'"
        )
        load_to_clickhouse(clean_df(df), f"trnx.{TABLE}")
        print(f"✓ Загружено {len(df)} записей в ClickHouse")
    else:
        print("✓ Нет данных для ClickHouse")


# ====================== FLOW ======================
@flow(name="Transactions ETL", log_prints=True)
def transactions_etl_flow(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """
    Основной ETL процесс для транзакций

    Args:
        start_date: Начальная дата (опционально, по умолчанию 7 дней назад)
        end_date: Конечная дата (опционально, по умолчанию сейчас)
    """
    print(f"\n=== ЗАГРУЗКА {TABLE.upper()} ===\n")

    df, s_date, e_date = extract(start_date, end_date)

    if df.empty:
        print("Нет данных. Завершение.")
        return

    to_minio(df, s_date, e_date)
    to_stage(df)
    merge_to_final()
    to_clickhouse(s_date, e_date)

    # Статистика
    df['date'] = pd.to_datetime(df['created_at']).dt.date
    print(f"\n✅ Завершено. Загружено {len(df)} транзакций за {df['date'].nunique()} дней")
    for date, count in df.groupby('date').size().items():
        print(f"   {date}: {count}")
    print()


# ====================== ЗАПУСК ======================
if __name__ == "__main__":
    # Вариант 1: Без параметров - последние 7 дней
    transactions_etl_flow()

    # Вариант 2: С конкретными датами
    # transactions_etl_flow(
    #     start_date=datetime(2024, 1, 1),
    #     end_date=datetime(2024, 1, 31)
    # )

    # Вариант 3: Только start_date (end_date = сейчас)
    # transactions_etl_flow(start_date=datetime(2024, 1, 1))

    # Вариант 4: Только end_date (start_date = end_date - 7 дней)
    # transactions_etl_flow(end_date=datetime(2024, 1, 31))