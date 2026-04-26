from prefect import task, flow
import pandas as pd
import numpy as np
import json
from minio import Minio
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Optional
from etl_helpers import pg_connection, load_to_clickhouse
from etl_config import config

TABLE = "transaction_pipeline_log"
DAYS = 7


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка DataFrame от NaT и NaN значений для PostgreSQL"""
    df = df.copy()

    # Обработка timestamp колонок
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].where(pd.notnull(df[col]), None)
        elif df[col].dtype == 'float64':
            if df[col].notna().all() and (df[col].dropna() == df[col].dropna().astype(int)).all():
                df[col] = df[col].astype('Int64')
            else:
                df[col] = df[col].where(pd.notnull(df[col]), None)
        elif df[col].dtype == 'int64':
            df[col] = df[col].where(pd.notnull(df[col]), None)
        elif df[col].dtype == 'object':
            # Для JSONB колонки - преобразуем в JSON строку
            if col == 'payload':
                df[col] = df[col].apply(lambda x: handle_json_field(x))
            else:
                df[col] = df[col].fillna('')
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NaN'], '')

    return df


def handle_json_field(value):
    """Правильная обработка JSONB полей"""
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, dict):
        # Если уже словарь, конвертируем в JSON строку
        return json.dumps(value)
    if isinstance(value, str):
        # Если строка, пробуем распарсить и снова преобразовать
        try:
            # Проверяем, валидный ли JSON
            parsed = json.loads(value)
            return json.dumps(parsed)
        except:
            # Если не валидный JSON, возвращаем как есть или пустой объект
            return json.dumps({"value": str(value)})
    return json.dumps({"value": str(value)})


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

    print(f"✓ Извлечено {len(df)} записей логов за {start.date()} - {end.date()}")
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
                        if val == int(val):
                            converted_row.append(int(val))
                        else:
                            converted_row.append(val)
                    elif isinstance(val, dict):
                        # Для словарей преобразуем в JSON строку
                        converted_row.append(json.dumps(val))
                    elif isinstance(val, str) and val.startswith('{'):
                        # Пробуем распарсить строку как JSON
                        try:
                            parsed = json.loads(val.replace("'", '"'))
                            converted_row.append(json.dumps(parsed))
                        except:
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
        INSERT INTO trnx_final.transaction_pipeline_log
        SELECT * FROM trnx_stage.transaction_pipeline_log
        ON CONFLICT (log_id) DO UPDATE SET
            transaction_id = EXCLUDED.transaction_id,
            step_name = EXCLUDED.step_name,
            status = EXCLUDED.status,
            payload = EXCLUDED.payload,
            created_at = EXCLUDED.created_at
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
        # Конвертируем все NaN и NaT в None для ClickHouse
        df = df.where(pd.notnull(df), None)

        # Для строковых колонок заменяем NaN на пустую строку
        for col in df.select_dtypes(include=['object']).columns:
            if col == 'payload':
                # Для JSONB преобразуем в строку
                df[col] = df[col].fillna('{}')
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else str(x))
            else:
                df[col] = df[col].fillna('')
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NaN', 'NaT'], '')

        # Для числовых колонок
        for col in df.select_dtypes(include=['float64', 'float32']).columns:
            df[col] = df[col].fillna(0)

        for col in df.select_dtypes(include=['int64', 'int32']).columns:
            df[col] = df[col].fillna(0)

        # Для datetime колонок
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = df[col].fillna(pd.Timestamp.now())

        # Загружаем в ClickHouse
        load_to_clickhouse(df, f"trnx.{TABLE}")
        print(f"✓ Загружено {len(df)} записей в ClickHouse")


@flow(name="Transaction Pipeline Log ETL", log_prints=True)
def transaction_pipeline_log_etl_flow(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """Основной ETL процесс для логов pipeline'а транзакций"""
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

    print(f"\n✅ Загружено {len(df)} записей логов за {s_date.date()} - {e_date.date()}\n")


if __name__ == "__main__":
    transaction_pipeline_log_etl_flow()