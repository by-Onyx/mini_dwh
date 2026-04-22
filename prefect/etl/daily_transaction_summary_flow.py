from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, timedelta
from etl_config import config
from etl_helpers import pg_connection, load_to_clickhouse

@task
def extract_daily_transactions(execution_date: date) -> pd.DataFrame:
    """Extract transactions for a given date from PostgreSQL."""
    with pg_connection("warehouse") as conn:
        query = f"""
        SELECT
            user_id,
            account_id,
            amount,
            currency,
            payment_method,
            status,
            type,
            created_at::date AS txn_date
        FROM {config.SCHEMA_FINAL}.transactions
        WHERE created_at::date = %s
        """
        df = pd.read_sql(query, conn, params=(execution_date,))
    print(f"✓ Extracted {len(df)} transactions for {execution_date}")
    return df

@task
def aggregate_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate transactions into daily summary."""
    if df.empty:
        # Return empty dataframe with correct schema
        return pd.DataFrame(columns=[
            "txn_date", "user_id", "currency", "payment_method",
            "total_amount", "txn_count", "success_count", "failed_count",
            "authorize_count", "capture_count", "refund_count"
        ])

    summary = df.groupby(["txn_date", "user_id", "currency", "payment_method"]).agg(
        total_amount=("amount", "sum"),
        txn_count=("transaction_id", "count") if "transaction_id" in df.columns else ("amount", "count"),
        success_count=("status", lambda x: (x == "success").sum()),
        failed_count=("status", lambda x: (x == "failed").sum()),
        authorize_count=("type", lambda x: (x == "authorize").sum()),
        capture_count=("type", lambda x: (x == "capture").sum()),
        refund_count=("type", lambda x: (x == "refund").sum())
    ).reset_index()

    return summary

@task
def load_summary_to_postgres(df: pd.DataFrame, table_name: str = "daily_transaction_summary"):
    """Upsert daily summary into PostgreSQL marts schema."""
    if df.empty:
        print("No data to load into PostgreSQL.")
        return

    engine = create_engine(config.PG_URL)
    with engine.begin() as conn:
        # Create marts schema and table if not exists
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS trnx_marts;"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS trnx_marts.{table_name} (
                txn_date DATE,
                user_id BIGINT,
                currency CHAR(3),
                payment_method VARCHAR(20),
                total_amount DECIMAL(15,2),
                txn_count INT,
                success_count INT,
                failed_count INT,
                authorize_count INT,
                capture_count INT,
                refund_count INT,
                PRIMARY KEY (txn_date, user_id, currency, payment_method)
            );
        """))

        # Insert with ON CONFLICT update
        cols = df.columns.tolist()
        placeholders = ", ".join(["%s"] * len(cols))
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols if col not in ("txn_date", "user_id", "currency", "payment_method")])
        sql = f"""
            INSERT INTO trnx_marts.{table_name} ({', '.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (txn_date, user_id, currency, payment_method) DO UPDATE SET
                {update_set}
        """
        conn.execute(text(sql), [tuple(row) for row in df.to_numpy()])

    print(f"✓ Loaded {len(df)} rows into trnx_marts.{table_name}")

@task
def load_summary_to_clickhouse(df: pd.DataFrame, table_name: str = "daily_transaction_summary"):
    """Load aggregated data to ClickHouse (append-only, or use ReplacingMergeTree)."""
    if df.empty:
        return
    # Ensure ClickHouse table exists (use ReplacingMergeTree for upsert behavior)
    config.CH_CLIENT.command(f"""
        CREATE TABLE IF NOT EXISTS trnx_marts.{table_name} (
            txn_date Date,
            user_id Int64,
            currency String,
            payment_method String,
            total_amount Decimal(15,2),
            txn_count UInt32,
            success_count UInt32,
            failed_count UInt32,
            authorize_count UInt32,
            capture_count UInt32,
            refund_count UInt32
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (txn_date, user_id, currency, payment_method)
    """)
    load_to_clickhouse(df, f"trnx_marts.{table_name}")

@flow(name="daily_transaction_summary_mart", log_prints=True)
def daily_transaction_summary_flow(execution_date: date = None):
    if execution_date is None:
        execution_date = date.today() - timedelta(days=1)  # Default to yesterday

    print(f"\n=== Building Daily Transaction Summary for {execution_date} ===\n")

    raw_df = extract_daily_transactions(execution_date)
    summary_df = aggregate_daily_summary(raw_df)
    load_summary_to_postgres(summary_df)
    load_summary_to_clickhouse(summary_df)

    print(f"\n✅ Daily summary mart built for {execution_date}\n")

if __name__ == "__main__":
    daily_transaction_summary_flow()