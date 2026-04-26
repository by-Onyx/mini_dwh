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


def clean_for_clickhouse(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка DataFrame для ClickHouse - заменяем NaN на пустые строки или None"""
    df = df.copy()

    for col in df.columns:
        # Для строковых колонок заменяем NaN на пустую строку
        if df[col].dtype == 'object':
            df[col] = df[col].fillna('')
            df[col] = df[col].astype(str)
            # Заменяем 'nan' и 'None' на пустую строку
            df[col] = df[col].replace(['nan', 'None', 'NaN'], '')

        # Для числовых колонок заменяем NaN на None (NULL в ClickHouse)
        elif df[col].dtype in ['float64', 'float32', 'int64', 'int32']:
            df[col] = df[col].where(pd.notnull(df[col]), None)
            # Для float заменяем NaN на None
            if df[col].dtype == 'float64':
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)

        # Для datetime колонок
        elif df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].where(pd.notnull(df[col]), None)

    return df


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка DataFrame от NaT и NaN значений для PostgreSQL"""
    df = df.copy()

    # Конвертируем enum колонки в строки
    for col in ['payment_method', 'status', 'type']:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Обработка timestamp колонок
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].where(pd.notnull(df[col]), None)
        elif df[col].dtype == 'float64':
            # Проверяем, может ли колонка быть целочисленной
            # Если все значения - целые числа, конвертируем в int
            if df[col].notna().all() and (df[col].dropna() == df[col].dropna().astype(int)).all():
                df[col] = df[col].astype('Int64')  # nullable int
            else:
                df[col] = df[col].where(pd.notnull(df[col]), None)
        elif df[col].dtype == 'int64':
            df[col] = df[col].where(pd.notnull(df[col]), None)
        elif df[col].dtype == 'object':
            df[col] = df[col].fillna('')
            # Не конвертируем числовые строки в обычные строки без потери типа
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['nan', 'None', 'NaN'], '')

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


@task
def to_stage(df: pd.DataFrame):
    """Загрузка в stage таблицу"""
    with pg_connection("warehouse") as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.SCHEMA_STAGE}.{TABLE}")

            cols = df.columns.tolist()
            placeholders = ', '.join(['%s'] * len(cols))
            sql = f"INSERT INTO {config.SCHEMA_STAGE}.{TABLE} ({', '.join(cols)}) VALUES ({placeholders})"

            data = []
            for row in df.to_numpy():
                converted_row = []
                for val in row:
                    if pd.isna(val):
                        converted_row.append(None)
                    elif isinstance(val, pd.Timestamp):
                        converted_row.append(val.to_pydatetime())
                    elif isinstance(val, (np.int64, np.int32, int)):
                        converted_row.append(int(val))
                    elif isinstance(val, (np.float64, np.float32, float)):
                        # Проверяем, является ли float целым числом
                        if val == int(val):
                            converted_row.append(int(val))
                        else:
                            converted_row.append(val)
                    else:
                        converted_row.append(str(val) if val is not None else None)
                data.append(tuple(converted_row))

            cur.executemany(sql, data)
            conn.commit()

    print(f"✓ Загружено {len(df)} записей в stage")


@task
def merge_to_final():
    """MERGE данных в final таблицу"""
    engine = create_engine(config.PG_URL)

    merge_sql = text("""
        INSERT INTO trnx_final.transactions 
        SELECT * FROM trnx_stage.transactions
        ON CONFLICT (transaction_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            account_id = EXCLUDED.account_id,
            amount = EXCLUDED.amount,
            currency = EXCLUDED.currency,
            payment_method = EXCLUDED.payment_method,
            card_id = EXCLUDED.card_id,
            cash_terminal_id = EXCLUDED.cash_terminal_id,
            status = EXCLUDED.status,
            type = EXCLUDED.type,
            retry_count = EXCLUDED.retry_count,
            last_error = EXCLUDED.last_error,
            external_txn_id = EXCLUDED.external_txn_id,
            created_at = EXCLUDED.created_at,
            processed_at = EXCLUDED.processed_at,
            updated_at = EXCLUDED.updated_at
    """)

    with engine.begin() as conn:
        conn.execute(merge_sql)

    with pg_connection("warehouse") as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.SCHEMA_STAGE}.{TABLE}")
            conn.commit()

    print(f"✓ MERGE выполнен, stage очищен")


@task
def to_clickhouse(start_date: datetime, end_date: datetime):
    """Загрузка в ClickHouse с правильной обработкой типов"""
    engine = create_engine(config.PG_URL)
    with engine.connect() as conn:
        df = pd.read_sql(
            f"SELECT * FROM {config.SCHEMA_FINAL}.{TABLE} WHERE created_at BETWEEN %s AND %s",
            conn.connection,
            params=[start_date, end_date]
        )

    if not df.empty:
        # Конвертируем все NaN и NaT в None для ClickHouse
        df = df.where(pd.notnull(df), None)

        # Для строковых колонок заменяем NaN на пустую строку
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].fillna('')
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['nan', 'None', 'NaN', 'NaT'], '')

        # Для числовых колонок оставляем как есть, но заменяем NaN на 0
        for col in df.select_dtypes(include=['float64', 'float32']).columns:
            df[col] = df[col].fillna(0)

        # Для int колонок
        for col in df.select_dtypes(include=['int64', 'int32']).columns:
            df[col] = df[col].fillna(0)

        # Для datetime колонок
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = df[col].fillna(pd.Timestamp.now())

        # Загружаем в ClickHouse
        load_to_clickhouse(df, f"trnx.{TABLE}")
        print(f"✓ Загружено {len(df)} записей в ClickHouse")


# ====================== FLOW ======================
@flow(name="Transactions ETL", log_prints=True)
def transactions_etl_flow(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """Основной ETL процесс для транзакций"""
    print(f"\n{'=' * 60}")
    print(f" ЗАГРУЗКА ТАБЛИЦЫ: {TABLE}")
    print(f"{'=' * 60}\n")

    df, s_date, e_date = extract(start_date, end_date)

    if df.empty:
        print("❌ Нет данных за указанный период. Завершение.")
        return

    to_minio(df, s_date, e_date)
    to_stage(df)
    merge_to_final()
    to_clickhouse(s_date, e_date)

    print(f"\n✅ Загружено {len(df)} транзакций за {s_date.date()} - {e_date.date()}\n")


if __name__ == "__main__":
    transactions_etl_flow()