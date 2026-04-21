from prefect import flow, task
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from etl_config import config
from etl_helpers import pg_connection

@task
def snapshot_account_balances(snapshot_date: date) -> pd.DataFrame:
    """Get current balances from accounts table (or compute from transactions)."""
    # Since balances are stored with version, we can just take the latest.
    with pg_connection("warehouse") as conn:
        query = f"""
        SELECT
            account_id,
            user_id,
            currency,
            balance,
            version,
            updated_at
        FROM {config.SCHEMA_FINAL}.accounts
        """
        df = pd.read_sql(query, conn)
    df['snapshot_date'] = snapshot_date
    return df

@task
def load_snapshot_to_postgres(df: pd.DataFrame, table_name: str = "account_balance_snapshot"):
    if df.empty:
        return
    engine = create_engine(config.PG_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS trnx_marts;"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS trnx_marts.{table_name} (
                account_id BIGINT,
                user_id BIGINT,
                currency CHAR(3),
                balance DECIMAL(15,2),
                version BIGINT,
                updated_at TIMESTAMP,
                snapshot_date DATE,
                PRIMARY KEY (account_id, snapshot_date)
            );
        """))
        # Insert with conflict handling
        cols = df.columns.tolist()
        placeholders = ", ".join(["%s"] * len(cols))
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols if col not in ("account_id", "snapshot_date")])
        sql = f"""
            INSERT INTO trnx_marts.{table_name} ({', '.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (account_id, snapshot_date) DO UPDATE SET
                {update_set}
        """
        conn.execute(text(sql), [tuple(row) for row in df.to_numpy()])
    print(f"✓ Loaded snapshot for {snapshot_date}")

@task
def load_snapshot_to_clickhouse(df: pd.DataFrame):
    if df.empty:
        return
    config.CH_CLIENT.command("""
        CREATE TABLE IF NOT EXISTS trnx_marts.account_balance_snapshot (
            account_id Int64,
            user_id Int64,
            currency String,
            balance Decimal(15,2),
            version Int64,
            updated_at DateTime,
            snapshot_date Date
        ) ENGINE = MergeTree()
        ORDER BY (account_id, snapshot_date)
    """)
    config.CH_CLIENT.insert_df("trnx_marts.account_balance_snapshot", df)
    print("✓ Loaded snapshot to ClickHouse")

@flow(name="account_balance_snapshot_mart", log_prints=True)
def account_balance_snapshot_flow(snapshot_date: date = None):
    if snapshot_date is None:
        snapshot_date = date.today()
    print(f"\n=== Taking account balance snapshot for {snapshot_date} ===\n")
    df = snapshot_account_balances(snapshot_date)
    load_snapshot_to_postgres(df)
    load_snapshot_to_clickhouse(df)
    print("\n✅ Balance snapshot completed.\n")

if __name__ == "__main__":
    account_balance_snapshot_flow()