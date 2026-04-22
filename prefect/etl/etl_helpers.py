import psycopg2
from contextlib import contextmanager
from etl_config import config


@contextmanager
def pg_connection(db_type="warehouse"):
    """
    Context manager for PostgreSQL connections.

    Args:
        db_type: Type of database connection ("warehouse" or "api")
    """
    if db_type == "api":
        conn_str = config.API_PG_URL
    else:  # default to warehouse
        conn_str = config.PG_URL

    conn = psycopg2.connect(conn_str)
    try:
        yield conn
    finally:
        conn.close()


def load_to_clickhouse(df, table_name, ch_client=None):
    """
    Load DataFrame to ClickHouse.

    Args:
        df: Pandas DataFrame to load
        table_name: Name of the ClickHouse table (with schema)
        ch_client: ClickHouse client (defaults to config.CH_CLIENT)
    """
    if ch_client is None:
        ch_client = config.CH_CLIENT

    if df.empty:
        return

    ch_client.insert_df(table_name, df)
    print(f"✓ Loaded {len(df)} rows to ClickHouse table {table_name}")