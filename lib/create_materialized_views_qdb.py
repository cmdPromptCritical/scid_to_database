"""
QuestDB Materialized Views Creation Script

This script creates and manages materialized views in QuestDB for trading data analysis.
It handles the creation of OHLC (Open, High, Low, Close) aggregated views with
15-second intervals for efficient querying and analysis.

Features:
- Creates trades_ohlc_15s materialized view with 15-second OHLC aggregation
- Handles dropping and recreating existing views
- Uses environment variables from qdb.env for database connection
- Provides error handling and logging

Usage:
    python create_materialized_views_qdb.py

Requirements:
    - QuestDB running and accessible
    - qdb.env file with database connection parameters
    - psycopg2 library installed
    - python-dotenv library installed

Environment Variables (from qdb.env):
    - DB_HOST: QuestDB host (default: localhost)
    - DB_USER: Database user (default: admin)
    - DB_PASSWORD: Database password (default: quest)
    - QUESTDB_PG_PORT: PostgreSQL wire protocol port (default: 8812)
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

# Load environment variables from qdb.env file
env_path = Path(__file__).parent.parent / 'qdb.env'
load_dotenv(env_path)

create_view_sql = """
CREATE MATERIALIZED VIEW IF NOT EXISTS trades_ohlc_15s
WITH BASE 'trades' REFRESH IMMEDIATE AS (
    SELECT
        time,
        symbol,
        symbol_period,
        first(close) AS open,
        max(close) AS high,
        min(close) AS low,
        last(close) AS close,
        sum(volume) AS volume,
        sum(number_of_trades) AS number_of_trades,
        sum(bid_volume) AS bid_volume,
        sum(ask_volume) AS ask_volume,
        front_contract
    FROM trades
    WHERE front_contract = true
    SAMPLE BY 15s
) PARTITION BY DAY;
"""

drop_view_sql = "DROP MATERIALIZED VIEW IF EXISTS trades_ohlc_15s;"

def get_db_connection():
    """Establishes a connection to the QuestDB database."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("QUESTDB_PG_PORT", "8812"),
            user=os.getenv("DB_USER", "admin"),
            password=os.getenv("DB_PASSWORD", "quest"),
            dbname="qdb"
        )
        print("Successfully connected to QuestDB.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to QuestDB: {e}", file=sys.stderr)
        sys.exit(1)

def drop_materialized_view(cursor):
    """Drops the existing materialized view if it exists."""
    try:
        print("Dropping existing materialized view 'trades_ohlc_15s' if it exists...")
        cursor.execute(drop_view_sql)
        print("Successfully dropped materialized view (if it existed).")
    except psycopg2.Error as e:
        print(f"Error dropping materialized view: {e}", file=sys.stderr)
        raise

def create_materialized_view(cursor):
    """Creates the new materialized view."""
    try:
        print("Creating new materialized view 'trades_ohlc_15s'...")
        cursor.execute(create_view_sql)
        print("Successfully created materialized view 'trades_ohlc_15s'.")
    except psycopg2.Error as e:
        print(f"Error creating materialized view: {e}", file=sys.stderr)
        raise

def main():
    """Main function to create the materialized view."""
    print("QuestDB Materialized Views Creation Script")
    print("=" * 50)

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Drop existing view if it exists
            drop_materialized_view(cursor)
            conn.commit()

            # Create new view
            create_materialized_view(cursor)
            conn.commit()

            print("\nMaterialized view creation completed successfully!")
            print("The 'trades_ohlc_15s' view is now available for querying.")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()