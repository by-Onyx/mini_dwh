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


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """Конвертация типов данных для PostgreSQL"""
    df = df.copy()

    # Определяем типы колонок для final таблицы
    column_types = {
        'transaction_id': 'int64',
        'user_id': 'int64',
        'account_id': 'int64',
        'amount': 'float64',
        'currency': 'object',
        'payment_method': 'object',
        'card_id': 'int64',
        'cash_terminal_id': 'object',
        'status': 'object',
        'type': 'object',
        'retry_count': 'int64',
        'last_error': 'object',
        'external_txn_id': 'object',
        'created_at': 'datetime64[ns]',
        'processed_at': 'datetime64[ns]',
        'updated_at': 'datetime64[ns]'
    }

    for col, dtype in column_types.items():
        if col in df.columns:
            if dtype == 'int64':
                # Для BIGINT: конвертируем в число, убираем .0
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0).astype('int64')
                # Убираем .0 если значение - число с плавающей точкой
                df[col] = df[col].apply(lambda x: int(float(x)) if pd.notnull(x) else None)

            elif dtype == 'float64':
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].where(pd.notnull(df[col]), None)

            elif dtype == 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].where(pd.notnull(df[col]), None)

            elif dtype == 'object':
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NaN', 'NaT', 'nat'], None)
                df[col] = df[col].apply(lambda x: None if x in ['nan', 'None', 'NaN', 'NaT', 'nat', ''] else x)

    return df


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка DataFrame от NaT и NaN значений"""
    df = df.copy()

    # Конвертируем enum колонки в строки
    for col in ['payment_method', 'status', 'type']:
        if col in df.columns:
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['nan', 'None', 'NaN'], 'card')

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

    # Применяем конвертацию типов
    df = convert_types(df)

    return df, start, end


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
    """Загрузка в stage таблицу с правильными типами"""
    df_clean = df.copy()

    with pg_connection("warehouse") as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {config.SCHEMA_STAGE}.{TABLE}")

            cols = df_clean.columns.tolist()
            placeholders = ', '.join(['%s'] * len(cols))
            sql = f"INSERT INTO {config.SCHEMA_STAGE}.{TABLE} ({', '.join(cols)}) VALUES ({placeholders})"

            data = []
            for idx, row in df_clean.iterrows():
                converted_row = []
                for col in cols:
                    val = row[col]

                    # Обработка разных типов
                    if pd.isna(val) or val == 'nan' or val == 'None' or val == '':
                        converted_row.append(None)
                    elif col in ['transaction_id', 'user_id', 'account_id', 'retry_count']:
                        # BIGINT поля - конвертируем в int
                        try:
                            # Убираем .0 если есть
                            if isinstance(val, float) and val.is_integer():
                                converted_row.append(int(val))
                            elif isinstance(val, str) and '.' in val:
                                converted_row.append(int(float(val)))
                            else:
                                converted_row.append(int(val) if val is not None else None)
                        except (ValueError, TypeError):
                            converted_row.append(None)
                    elif col == 'amount':
                        # DECIMAL поле
                        try:
                            converted_row.append(float(val) if val is not None else None)
                        except (ValueError, TypeError):
                            converted_row.append(None)
                    elif col in ['created_at', 'processed_at', 'updated_at']:
                        # TIMESTAMP поля
                        if isinstance(val, pd.Timestamp):
                            converted_row.append(val.to_pydatetime())
                        elif isinstance(val, datetime):
                            converted_row.append(val)
                        else:
                            try:
                                converted_row.append(pd.to_datetime(val).to_pydatetime() if val else None)
                            except:
                                converted_row.append(None)
                    else:
                        # TEXT поля
                        converted_row.append(str(val) if val is not None else None)

                data.append(tuple(converted_row))

            cur.executemany(sql, data)
            conn.commit()

    print(f"✓ Загружено {len(data)} записей в stage")


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
    """Загрузка в ClickHouse"""
    engine = create_engine(config.PG_URL)
    with engine.connect() as conn:
        df = pd.read_sql(
            f"SELECT * FROM {config.SCHEMA_FINAL}.{TABLE} WHERE created_at BETWEEN %s AND %s",
            conn.connection,
            params=[start_date, end_date]
        )

    if not df.empty:
        # Конвертируем типы для ClickHouse
        for col in df.columns:
            if col in ['transaction_id', 'user_id', 'account_id', 'card_id', 'retry_count']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0).astype('int64')
            elif col == 'amount':
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0.0)
            elif col in ['created_at', 'processed_at', 'updated_at']:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            else:
                df[col] = df[col].astype(str).fillna('')
                df[col] = df[col].replace(['nan', 'None', 'NaN', 'NaT'], '')

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

    print(f"\nТипы данных после конвертации:")
    print(df.dtypes)

    to_minio(df, s_date, e_date)
    to_stage(df)
    merge_to_final()
    to_clickhouse(s_date, e_date)

    print(f"\n✅ Загружено {len(df)} транзакций за {s_date.date()} - {e_date.date()}\n")


if __name__ == "__main__":
    transactions_etl_flow()