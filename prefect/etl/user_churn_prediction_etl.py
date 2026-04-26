# etl_user_churn_prediction.py
from prefect import task, flow, get_run_logger
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from etl_helpers import pg_connection, load_to_clickhouse
from etl_config import config

TABLE = "user_churn_prediction"


@task(retries=2, retry_delay_seconds=30)
def execute_churn_procedure(
        model_version: str = "v1.0",
        lookback_days: int = 90,
        high_risk_threshold: float = 0.8,
        medium_risk_threshold: float = 0.5
) -> Dict[str, Any]:
    """
    Выполнение процедуры расчета прогноза оттока пользователей
    """
    logger = get_run_logger()

    engine = create_engine(config.PG_URL)

    with engine.connect() as conn:
        # Выполняем процедуру
        logger.info(f"Запуск процедуры calculate_user_churn_prediction с параметрами: "
                    f"model_version={model_version}, lookback_days={lookback_days}, "
                    f"high_threshold={high_risk_threshold}, medium_threshold={medium_risk_threshold}")

        conn.execute(
            text("""
                CALL trnx.calculate_user_churn_prediction(
                    :model_version,
                    :lookback_days,
                    :high_threshold,
                    :medium_threshold
                )
            """),
            {
                "model_version": model_version,
                "lookback_days": lookback_days,
                "high_threshold": high_risk_threshold,
                "medium_threshold": medium_risk_threshold
            }
        )
        conn.commit()

        # Получаем дату расчета
        result = conn.execute(
            text("""
                SELECT 
                    MAX(calculation_date) as calculation_date,
                    COUNT(*) as total_predictions
                FROM trnx.user_churn_prediction
            """)
        ).fetchone()

        logger.info(f"Процедура выполнена успешно. Дата расчета: {result.calculation_date}, "
                    f"всего записей: {result.total_predictions}")

        return {
            "calculation_date": result.calculation_date,
            "total_predictions": result.total_predictions
        }


@task
def extract_from_postgres(calculation_date: datetime) -> pd.DataFrame:
    """
    Извлечение данных прогноза из PostgreSQL
    """
    logger = get_run_logger()

    logger.info(f"Извлечение данных прогноза за {calculation_date}")

    with pg_connection("warehouse") as conn:
        df = pd.read_sql(
            text("""
                SELECT 
                    prediction_id,
                    calculation_date,
                    user_id,
                    user_age_days,
                    total_transactions,
                    days_since_last_txn,
                    avg_transaction_amount,
                    failed_transactions,
                    churn_probability,
                    churn_risk_category,
                    is_blocked,
                    prediction_date,
                    model_version
                FROM trnx.user_churn_prediction
                WHERE calculation_date = :calc_date
                ORDER BY churn_probability DESC
            """),
            conn,
            params={"calc_date": calculation_date}
        )

    logger.info(f"Извлечено {len(df)} записей из PostgreSQL")
    return df


@task
def clean_for_clickhouse(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка DataFrame для ClickHouse"""
    logger = get_run_logger()
    df = df.copy()

    # Заменяем NaN на подходящие значения
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].fillna('')
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(['nan', 'None', 'NaN', 'NaT'], '')
        elif df[col].dtype == 'float64':
            df[col] = df[col].fillna(0)
        elif df[col].dtype == 'int64':
            df[col] = df[col].fillna(0)
        elif df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].fillna(pd.Timestamp.now())
        elif df[col].dtype == 'bool':
            df[col] = df[col].fillna(False).astype(int)

    # Конвертируем boolean в int для ClickHouse
    if 'is_blocked' in df.columns:
        df['is_blocked'] = df['is_blocked'].astype(int)

    # Удаляем prediction_id для ClickHouse (если не нужен)
    if 'prediction_id' in df.columns:
        df = df.drop(columns=['prediction_id'])

    logger.info(f"Подготовлено {len(df)} записей для ClickHouse")
    return df


@task
def load_to_clickhouse_table(df: pd.DataFrame):
    """Загрузка данных в ClickHouse"""
    logger = get_run_logger()

    if df.empty:
        logger.warning("Нет данных для загрузки в ClickHouse")
        return

    df_clean = clean_for_clickhouse(df)

    # Загружаем в ClickHouse
    load_to_clickhouse(df_clean, f"trnx.{TABLE}")

    logger.info(f"Загружено {len(df_clean)} записей в ClickHouse таблицу trnx.{TABLE}")


@task
def log_statistics(df: pd.DataFrame, calculation_date: datetime):
    """Логирование статистики по прогнозам"""
    logger = get_run_logger()

    stats = df.groupby('churn_risk_category').agg({
        'user_id': 'count',
        'churn_probability': 'mean',
        'total_transactions': 'mean',
        'days_since_last_txn': 'mean'
    }).round(2)

    logger.info("=" * 60)
    logger.info(f"СТАТИСТИКА ПРОГНОЗА ОТТОКА за {calculation_date}")
    logger.info("=" * 60)

    for category in ['HIGH_RISK_CHURN', 'MEDIUM_RISK_CHURN', 'LOW_RISK_CHURN']:
        if category in stats.index:
            row = stats.loc[category]
            logger.info(f"{category}:")
            logger.info(f"  - Пользователей: {int(row['user_id'])}")
            logger.info(f"  - Средняя вероятность: {row['churn_probability'] * 100:.1f}%")
            logger.info(f"  - Среднее кол-во транзакций: {row['total_transactions']:.1f}")
            logger.info(f"  - Средняя неактивность: {row['days_since_last_txn']:.1f} дней")
            logger.info("")

    # Особое внимание на high risk
    high_risk_count = len(df[df['churn_risk_category'] == 'HIGH_RISK_CHURN'])
    if high_risk_count > 100:
        logger.warning(f"ВНИМАНИЕ: {high_risk_count} пользователей в зоне высокого риска оттока!")


@flow(name="User Churn Prediction ETL", log_prints=True)
def user_churn_prediction_flow(
        model_version: str = "v1.0",
        lookback_days: int = 90,
        high_risk_threshold: float = 0.8,
        medium_risk_threshold: float = 0.5
):
    """
    ETL процесс для прогнозирования оттока пользователей:
    1. Запуск процедуры расчета в PostgreSQL
    2. Извлечение результатов
    3. Загрузка в ClickHouse
    """
    logger = get_run_logger()

    print(f"\n{'=' * 60}")
    print(f" ПРОГНОЗИРОВАНИЕ ОТТОКА ПОЛЬЗОВАТЕЛЕЙ")
    print(f"{'=' * 60}\n")

    # 1. Запускаем процедуру
    result = execute_churn_procedure(
        model_version=model_version,
        lookback_days=lookback_days,
        high_risk_threshold=high_risk_threshold,
        medium_risk_threshold=medium_risk_threshold
    )

    calculation_date = result['calculation_date']
    total_predictions = result['total_predictions']

    if total_predictions == 0:
        logger.error("❌ Нет данных для прогнозирования. Завершение.")
        return

    # 2. Извлекаем данные из PostgreSQL
    df = extract_from_postgres(calculation_date)

    # 3. Логируем статистику
    log_statistics(df, calculation_date)

    # 4. Загружаем в ClickHouse
    load_to_clickhouse_table(df)

    print(f"\n✅ Прогноз оттока успешно загружен в ClickHouse")
    print(f"   Дата расчета: {calculation_date}")
    print(f"   Всего пользователей: {total_predictions}\n")


if __name__ == "__main__":
    # Запуск с параметрами по умолчанию
    user_churn_prediction_flow()

    # Или с кастомными параметрами:
    # user_churn_prediction_flow(
    #     model_version="v2.0",
    #     lookback_days=60,
    #     high_risk_threshold=0.75,
    #     medium_risk_threshold=0.45
    # )