#!/usr/bin/env python3
"""
Database Investigation Script for QuestDB

This script investigates the contents of the QuestDB database to help diagnose
data availability issues. It checks:
- Available tables and views
- Data structure and schema
- Date ranges and available symbols
- Sample data from trades table and materialized view
- Potential issues with data filtering

Usage:
    python investigate_database.py [--symbol SYMBOL] [--date DATE]

Arguments:
    --symbol: Optional symbol to investigate specifically
    --date: Optional date to investigate in YYYY-MM-DD format

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
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import psycopg2

# Load environment variables from qdb.env file
env_path = Path(__file__).parent / 'qdb.env'
load_dotenv(env_path)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Investigate QuestDB database contents',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python investigate_database.py
  python investigate_database.py --symbol ES
  python investigate_database.py --date 2025-08-21
  python investigate_database.py --symbol ES --date 2025-08-21
        """
    )

    parser.add_argument(
        '--symbol',
        help='Symbol to investigate specifically (e.g., ES, AAPL)'
    )

    parser.add_argument(
        '--date',
        help='Date to investigate in YYYY-MM-DD format'
    )

    return parser.parse_args()

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
        print("✅ Successfully connected to QuestDB.")
        return conn
    except psycopg2.Error as e:
        print(f"❌ Error connecting to QuestDB: {e}", file=sys.stderr)
        sys.exit(1)

def execute_query(conn, query, description=""):
    """Execute a query and return results as DataFrame."""
    try:
        if description:
            print(f"\n🔍 {description}")

        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

        if not rows:
            print("   No results found.")
            return None

        df = pd.DataFrame(rows, columns=columns)
        print(f"   Found {len(df)} rows")
        return df

    except psycopg2.Error as e:
        print(f"❌ Error executing query: {e}", file=sys.stderr)
        return None

def show_table_info(conn):
    """Show information about available tables and views."""
    print("\n" + "="*60)
    print("📋 DATABASE TABLES AND VIEWS")
    print("="*60)

    # Get all tables and views
    query = """
    SELECT
        name,
        type,
        designatedTimestamp,
        partitionBy,
        maxUncommittedRows,
        commitLag
    FROM tables()
    ORDER BY name
    """

    df = execute_query(conn, query, "Getting table information...")
    if df is not None:
        print("\n   Available tables/views:")
        for _, row in df.iterrows():
            print(f"   - {row['name']} ({row['type']})")
            if pd.notna(row['designatedTimestamp']):
                print(f"     Designated timestamp: {row['designatedTimestamp']}")
            if pd.notna(row['partitionBy']):
                print(f"     Partitioned by: {row['partitionBy']}")

def show_trades_table_info(conn, symbol_filter=None, date_filter=None):
    """Show information about the trades table."""
    print("\n" + "="*60)
    print("📊 TRADES TABLE INVESTIGATION")
    print("="*60)

    # Get table schema
    query = """
    SELECT
        column,
        type,
        indexed
    FROM table_columns('trades')
    ORDER BY column
    """

    df = execute_query(conn, query, "Getting trades table schema...")
    if df is not None:
        print("\n   Table schema:")
        for _, row in df.iterrows():
            indexed = " (indexed)" if row['indexed'] else ""
            print(f"   - {row['column']}: {row['type']}{indexed}")

    # Get data statistics
    query = """
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT symbol) as unique_symbols,
        MIN(time) as earliest_time,
        MAX(time) as latest_time
    FROM trades
    """

    df = execute_query(conn, query, "Getting trades table statistics...")
    if df is not None:
        row = df.iloc[0]
        print("\n   Table statistics:")
        print(f"   - Total rows: {row['total_rows']:,}")
        print(f"   - Unique symbols: {row['unique_symbols']}")
        print(f"   - Earliest timestamp: {row['earliest_time']}")
        print(f"   - Latest timestamp: {row['latest_time']}")

    # Get symbol counts
    query = """
    SELECT
        symbol,
        COUNT(*) as row_count,
        MIN(time) as earliest_time,
        MAX(time) as latest_time
    FROM trades
    GROUP BY symbol
    ORDER BY row_count DESC
    LIMIT 20
    """

    df = execute_query(conn, query, "Getting symbol distribution...")
    if df is not None:
        print("\n   Top symbols by row count:")
        for _, row in df.iterrows():
            print(f"   - {row['symbol']}: {row['row_count']:,} rows "
                  f"({row['earliest_time']} to {row['latest_time']})")

    # Get sample data
    query = """
    SELECT *
    FROM trades
    ORDER BY time DESC
    LIMIT 10
    """

    df = execute_query(conn, query, "Getting recent sample data from trades table...")
    if df is not None:
        print("\n   Recent sample data:")
        print(df.to_string(index=False))

    # Filter by symbol if specified
    if symbol_filter:
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            MIN(time) as earliest_time,
            MAX(time) as latest_time
        FROM trades
        WHERE symbol = '{symbol_filter}'
        """

        df = execute_query(conn, query, f"Getting data for symbol '{symbol_filter}'...")
        if df is not None:
            row = df.iloc[0]
            print(f"\n   Symbol '{symbol_filter}' statistics:")
            print(f"   - Total rows: {row['total_rows']:,}")
            print(f"   - Earliest timestamp: {row['earliest_time']}")
            print(f"   - Latest timestamp: {row['latest_time']}")

        # Get sample data for specific symbol
        query = f"""
        SELECT *
        FROM trades
        WHERE symbol = '{symbol_filter}'
        ORDER BY time DESC
        LIMIT 5
        """

        df = execute_query(conn, query, f"Getting recent sample data for symbol '{symbol_filter}'...")
        if df is not None:
            print(f"\n   Recent sample data for '{symbol_filter}':")
            print(df.to_string(index=False))

def show_materialized_view_info(conn, symbol_filter=None, date_filter=None):
    """Show information about the materialized view."""
    print("\n" + "="*60)
    print("📈 MATERIALIZED VIEW INVESTIGATION")
    print("="*60)

    # Check if materialized view exists
    query = """
    SELECT name, type
    FROM tables()
    WHERE name = 'trades_ohlc_15s'
    """

    df = execute_query(conn, query, "Checking if trades_ohlc_15s materialized view exists...")
    if df is None or df.empty:
        print("❌ Materialized view 'trades_ohlc_15s' does not exist!")
        print("   You may need to run the materialized view creation script first.")
        return

    print("✅ Materialized view 'trades_ohlc_15s' exists.")

    # Get materialized view statistics
    query = """
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT symbol) as unique_symbols,
        MIN(time) as earliest_time,
        MAX(time) as latest_time
    FROM trades_ohlc_15s
    """

    df = execute_query(conn, query, "Getting materialized view statistics...")
    if df is not None:
        row = df.iloc[0]
        print("   Materialized view statistics:")
        print(f"   - Total rows: {row['total_rows']:,}")
        print(f"   - Unique symbols: {row['unique_symbols']}")
        print(f"   - Earliest timestamp: {row['earliest_time']}")
        print(f"   - Latest timestamp: {row['latest_time']}")

    # Get symbol counts in materialized view
    query = """
    SELECT
        symbol,
        COUNT(*) as row_count,
        MIN(time) as earliest_time,
        MAX(time) as latest_time
    FROM trades_ohlc_15s
    GROUP BY symbol
    ORDER BY row_count DESC
    LIMIT 20
    """

    df = execute_query(conn, query, "Getting symbol distribution in materialized view...")
    if df is not None:
        print("   Top symbols in materialized view:")
        for _, row in df.iterrows():
            print(f"   - {row['symbol']}: {row['row_count']:,} rows "
                  f"({row['earliest_time']} to {row['latest_time']})")

    # Test the exact query used in the candlestick script
    test_query = """
    SELECT *
    FROM trades_ohlc_15s
    WHERE
        hour(to_timezone(time, 'America/New_York')) >= 9
        AND (
            hour(to_timezone(time, 'America/New_York')) > 9
            OR minute(to_timezone(time, 'America/New_York')) >= 30
        )
        AND hour(to_timezone(time, 'America/New_York')) < 16
        AND to_timezone(time, 'America/New_York')::date = '2025-08-21'
    ORDER BY time, symbol
    LIMIT 10
    """

    df = execute_query(conn, test_query, "Testing the candlestick query for 2025-08-21...")
    if df is not None:
        print("   Sample data from candlestick query:")
        print(df.to_string(index=False))

    # Filter by symbol if specified
    if symbol_filter:
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            MIN(time) as earliest_time,
            MAX(time) as latest_time
        FROM trades_ohlc_15s
        WHERE symbol = '{symbol_filter}'
        """

        df = execute_query(conn, query, f"Getting materialized view data for symbol '{symbol_filter}'...")
        if df is not None:
            row = df.iloc[0]
            print(f"\n   Materialized view data for '{symbol_filter}':")
            print(f"   - Total rows: {row['total_rows']:,}")
            print(f"   - Earliest timestamp: {row['earliest_time']}")
            print(f"   - Latest timestamp: {row['latest_time']}")

        # Test symbol-specific query
        test_query = f"""
        SELECT *
        FROM trades_ohlc_15s
        WHERE
            hour(to_timezone(time, 'America/New_York')) >= 9
            AND (
                hour(to_timezone(time, 'America/New_York')) > 9
                OR minute(to_timezone(time, 'America/New_York')) >= 30
            )
            AND hour(to_timezone(time, 'America/New_York')) < 16
            AND to_timezone(time, 'America/New_York')::date = '2025-08-21'
            AND symbol = '{symbol_filter}'
        ORDER BY time
        LIMIT 10
        """

        df = execute_query(conn, test_query, f"Testing candlestick query for '{symbol_filter}' on 2025-08-21...")
        if df is not None:
            print(f"\n   Sample data for '{symbol_filter}' on 2025-08-21:")
            print(df.to_string(index=False))

def show_timezone_analysis(conn):
    """Analyze timezone handling and trading hours."""
    print("\n" + "="*60)
    print("🕐 TIMEZONE AND TRADING HOURS ANALYSIS")
    print("="*60)

    # Check data distribution by hour in different timezones
    query = """
    SELECT
        hour(time) as utc_hour,
        hour(to_timezone(time, 'America/New_York')) as ny_hour,
        COUNT(*) as row_count
    FROM trades_ohlc_15s
    GROUP BY hour(time), hour(to_timezone(time, 'America/New_York'))
    ORDER BY ny_hour
    """

    df = execute_query(conn, query, "Analyzing data distribution by hour (NY timezone)...")
    if df is not None:
        print("   Data distribution by hour (NY timezone):")
        for _, row in df.iterrows():
            print(f"   - NY Hour {row['ny_hour']:2d} (UTC {row['utc_hour']:2d}): {row['row_count']:,} rows")

    # Check trading days
    query = """
    SELECT
        to_timezone(time, 'America/New_York')::date as ny_date,
        COUNT(*) as row_count
    FROM trades_ohlc_15s
    GROUP BY to_timezone(time, 'America/New_York')::date
    ORDER BY ny_date DESC
    LIMIT 10
    """

    df = execute_query(conn, query, "Checking recent trading days...")
    if df is not None:
        print("   Recent trading days:")
        for _, row in df.iterrows():
            print(f"   - {row['ny_date']}: {row['row_count']:,} rows")

def main():
    """Main function to investigate the database."""
    print("🔍 QuestDB Database Investigation Script")
    print("=" * 60)

    # Parse command line arguments
    args = parse_arguments()

    # Connect to database
    conn = get_db_connection()

    try:
        # Show table information
        show_table_info(conn)

        # Show trades table information
        show_trades_table_info(conn, args.symbol, args.date)

        # Show materialized view information
        show_materialized_view_info(conn, args.symbol, args.date)

        # Show timezone analysis
        show_timezone_analysis(conn)

        print("\n" + "="*60)
        print("✅ Database investigation complete!")
        print("="*60)

        if args.symbol:
            print(f"\n💡 Tips for symbol '{args.symbol}':")
            print("   - Check if the symbol exists in the database")
            print("   - Verify the date range contains trading data")
            print("   - Ensure the materialized view is up to date")
        else:
            print("💡 General tips:")
            print("   - Run the materialized view creation script if needed")
            print("   - Check if there's data in the base 'trades' table")
            print("   - Verify QuestDB is running and accessible")

    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if conn:
            conn.close()
            print("🔌 Database connection closed.")

if __name__ == "__main__":
    main()