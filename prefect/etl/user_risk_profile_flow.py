from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine, text
from config import config
from etl_helpers import pg_connection

@task
def extract_user_risk_data() -> pd.DataFrame:
    """Join users, cards, fingerprints, and recent transactions to compute risk factors."""
    with pg_connection("warehouse") as conn:
        query = f"""
        WITH user_card_risk AS (
            SELECT
                u.user_id,
                u.is_blocked,
                AVG(cf.risk_score) AS avg_fingerprint_risk
            FROM {config.SCHEMA_FINAL}.users u
            LEFT JOIN {config.SCHEMA_FINAL}.payment_cards pc ON u.user_id = pc.user_id
            LEFT JOIN {config.SCHEMA_FINAL}.card_fingerprints cf ON pc.card_id = cf.card_id
            GROUP BY u.user_id, u.is_blocked
        ),
        recent_failures AS (
            SELECT
                user_id,
                COUNT(*) AS failed_txn_last_30d
            FROM {config.SCHEMA_FINAL}.transactions
            WHERE status = 'failed'
              AND created_at >= NOW() - INTERVAL '30 days'
            GROUP BY user_id
        )
        SELECT
            ucr.user_id,
            ucr.is_blocked,
            COALESCE(ucr.avg_fingerprint_risk, 0) AS avg_fingerprint_risk,
            COALESCE(rf.failed_txn_last_30d, 0) AS failed_txn_last_30d,
            -- Simple risk score formula (adjustable)
            (CASE WHEN ucr.is_blocked THEN 100 ELSE 0 END +
             COALESCE(ucr.avg_fingerprint_risk, 0) * 0.5 +
             COALESCE(rf.failed_txn_last_30d, 0) * 2) AS total_risk_score
        FROM user_card_risk ucr
        LEFT JOIN recent_failures rf ON ucr.user_id = rf.user_id
        """
        df = pd.read_sql(query, conn)
    print(f"✓ Computed risk scores for {len(df)} users")
    return df

@task
def load_risk_profile_to_postgres(df: pd.DataFrame):
    """Replace entire table with new risk scores."""
    if df.empty:
        return
    engine = create_engine(config.PG_URL)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS trnx_marts;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS trnx_marts.user_risk_profile (
                user_id BIGINT PRIMARY KEY,
                is_blocked BOOLEAN,
                avg_fingerprint_risk DECIMAL(5,2),
                failed_txn_last_30d INT,
                total_risk_score DECIMAL(10,2),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        # Truncate and reload (or UPSERT)
        conn.execute(text("TRUNCATE TABLE trnx_marts.user_risk_profile;"))
        df.to_sql("user_risk_profile", conn, schema="trnx_marts", if_exists="append", index=False)
    print(f"✓ Loaded {len(df)} risk profiles into trnx_marts.user_risk_profile")

@task
def load_risk_to_clickhouse(df: pd.DataFrame):
    if df.empty:
        return
    config.CH_CLIENT.command("""
        CREATE TABLE IF NOT EXISTS trnx_marts.user_risk_profile (
            user_id Int64,
            is_blocked UInt8,
            avg_fingerprint_risk Decimal(5,2),
            failed_txn_last_30d UInt32,
            total_risk_score Decimal(10,2),
            updated_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree()
        ORDER BY user_id
    """)
    df['updated_at'] = pd.Timestamp.now()
    config.CH_CLIENT.insert_df("trnx_marts.user_risk_profile", df)
    print("✓ Loaded risk profiles to ClickHouse")

@flow(name="user_risk_profile_mart", log_prints=True)
def user_risk_profile_flow():
    print("\n=== Building User Risk Profile Mart ===\n")
    df = extract_user_risk_data()
    load_risk_profile_to_postgres(df)
    load_risk_to_clickhouse(df)
    print("\n✅ User risk mart updated.\n")

if __name__ == "__main__":
    user_risk_profile_flow()