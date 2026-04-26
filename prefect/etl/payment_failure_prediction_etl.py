from prefect import task, flow, get_run_logger
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from typing import Dict, Any
from etl_helpers import pg_connection, load_to_clickhouse
from etl_config import config

TABLE = "payment_failure_predictions"


@task(retries=2, retry_delay_seconds=30)
def execute_prediction_procedure() -> Dict[str, Any]:
    """
    Выполнение процедуры прогнозирования отказа платежей
    """
    logger = get_run_logger()

    engine = create_engine(config.PG_URL)

    with engine.connect() as conn:
        logger.info("Запуск процедуры trnx.predict_payment_failures()")

        conn.execute(text("CALL trnx.predict_payment_failures()"))
        conn.commit()

        # Получаем статистику выполнения
        result = conn.execute(
            text("""
                SELECT 
                    MAX(calculation_date) as calculation_date,
                    COUNT(*) as total_predictions,
                    COUNT(CASE WHEN risk_category = 'HIGH_RISK' THEN 1 END) as high_risk_count,
                    COUNT(CASE WHEN risk_category = 'MEDIUM_RISK' THEN 1 END) as medium_risk_count,
                    COUNT(CASE WHEN risk_category = 'LOW_RISK' THEN 1 END) as low_risk_count,
                    ROUND(AVG(failure_probability), 2) as avg_probability
                FROM trnx.payment_failure_predictions
                WHERE calculation_date = (
                    SELECT MAX(calculation_date) FROM trnx.payment_failure_predictions
                )
            """)
        ).fetchone()

        logger.info(f"Процедура выполнена успешно")
        logger.info(f"  - Дата расчета: {result.calculation_date}")
        logger.info(f"  - Всего прогнозов: {result.total_predictions}")
        logger.info(f"  - HIGH_RISK: {result.high_risk_count}")
        logger.info(f"  - MEDIUM_RISK: {result.medium_risk_count}")
        logger.info(f"  - LOW_RISK: {result.low_risk_count}")
        logger.info(f"  - Средняя вероятность отказа: {result.avg_probability}")

        return {
            "calculation_date": result.calculation_date,
            "total_predictions": result.total_predictions,
            "high_risk_count": result.high_risk_count,
            "medium_risk_count": result.medium_risk_count,
            "low_risk_count": result.low_risk_count,
            "avg_probability": float(result.avg_probability) if result.avg_probability else 0
        }


@task
def extract_from_postgres(calculation_date: datetime) -> pd.DataFrame:
    """
    Извлечение данных прогнозов из PostgreSQL
    """
    logger = get_run_logger()

    logger.info(f"Извлечение данных прогнозов за {calculation_date}")

    engine = create_engine(config.PG_URL)

    with engine.connect() as conn:
        query = f"""
            SELECT 
                prediction_id,
                calculation_date,
                transaction_id,
                user_id,
                amount,
                failure_probability,
                risk_category,
                recommendation,
                created_at
            FROM trnx.payment_failure_predictions
            WHERE calculation_date = '{calculation_date}'
            ORDER BY failure_probability DESC
        """

        df = pd.read_sql(query, conn)

    logger.info(f"Извлечено {len(df)} записей из PostgreSQL")
    return df


@task
def clean_for_clickhouse(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка DataFrame для ClickHouse"""
    logger = get_run_logger()
    df = df.copy()

    # Удаляем не нужные для ClickHouse колонки
    if 'prediction_id' in df.columns:
        df = df.drop(columns=['prediction_id'])
    if 'created_at' in df.columns:
        df = df.drop(columns=['created_at'])

    # Конвертация типов
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

    logger.info(f"Подготовлено {len(df)} записей для ClickHouse")
    logger.info(f"Колонки: {list(df.columns)}")

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
def log_high_risk_transactions(df: pd.DataFrame):
    """Логирование транзакций с высоким риском"""
    logger = get_run_logger()

    high_risk = df[df['risk_category'] == 'HIGH_RISK']

    if not high_risk.empty:
        logger.warning(f"⚠️ Обнаружено {len(high_risk)} транзакций с высоким риском отказа:")

        for _, row in high_risk.head(10).iterrows():
            logger.warning(f"  - Транзакция {row['transaction_id']}: "
                           f"сумма={row['amount']}, "
                           f"вероятность отказа={row['failure_probability']}, "
                           f"рекомендация={row['recommendation']}")

        if len(high_risk) > 10:
            logger.warning(f"  ... и еще {len(high_risk) - 10} транзакций")


@task
def log_statistics(df: pd.DataFrame, calculation_date: datetime):
    """Логирование статистики по прогнозам"""
    logger = get_run_logger()

    stats = df.groupby('risk_category').agg({
        'transaction_id': 'count',
        'failure_probability': 'mean',
        'amount': 'mean'
    }).round(2)

    logger.info("=" * 60)
    logger.info(f"СТАТИСТИКА ПРОГНОЗОВ ОТКАЗОВ за {calculation_date}")
    logger.info("=" * 60)

    for category in ['HIGH_RISK', 'MEDIUM_RISK', 'LOW_RISK']:
        if category in stats.index:
            row = stats.loc[category]
            logger.info(f"{category}:")
            logger.info(f"  - Транзакций: {int(row['transaction_id'])}")
            logger.info(f"  - Средняя вероятность отказа: {row['failure_probability'] * 100:.1f}%")
            logger.info(f"  - Средняя сумма: {row['amount']:.2f}")
            logger.info("")

    # Рекомендации по действиям
    high_risk_count = len(df[df['risk_category'] == 'HIGH_RISK'])
    if high_risk_count > 0:
        logger.warning(f"🔴 Рекомендуется проверить {high_risk_count} транзакций с высоким риском")

        recommendations = df[df['risk_category'] == 'HIGH_RISK']['recommendation'].value_counts()
        for rec, count in recommendations.items():
            logger.warning(f"   - {rec}: {count}")


@flow(name="Payment Failure Prediction ETL", log_prints=True)
def payment_failure_prediction_flow():
    """
    ETL процесс для прогнозирования отказа платежей:
    1. Запуск процедуры расчета в PostgreSQL
    2. Извлечение результатов
    3. Загрузка в ClickHouse
    """
    print(f"\n{'=' * 60}")
    print(f" ПРОГНОЗИРОВАНИЕ ОТКАЗОВ ПЛАТЕЖЕЙ")
    print(f"{'=' * 60}\n")

    # 1. Запускаем процедуру
    result = execute_prediction_procedure()

    calculation_date = result['calculation_date']
    total_predictions = result['total_predictions']

    if total_predictions == 0:
        print("❌ Нет транзакций для прогнозирования. Завершение.")
        return

    # 2. Извлекаем данные из PostgreSQL
    df = extract_from_postgres(calculation_date)

    # 3. Логируем статистику
    log_statistics(df, calculation_date)

    # 4. Логируем高风险 транзакции
    log_high_risk_transactions(df)

    # 5. Загружаем в ClickHouse
    load_to_clickhouse_table(df)

    print(f"\n✅ Прогнозы отказов платежей успешно загружены в ClickHouse")
    print(f"   Дата расчета: {calculation_date}")
    print(f"   Всего транзакций: {total_predictions}")
    print(f"   HIGH_RISK: {result['high_risk_count']}")
    print(f"   MEDIUM_RISK: {result['medium_risk_count']}")
    print(f"   LOW_RISK: {result['low_risk_count']}\n")


if __name__ == "__main__":
    payment_failure_prediction_flow()