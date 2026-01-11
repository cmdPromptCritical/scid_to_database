"""
QuestDB Front Contract Computation Script

This script determines the front contract of futures contract series in QuestDB by
identifying and marking the primary contract for each symbol on each trading day.
For each symbol and date, it sets the 'front_contract' flag to True for the
symbol_period with the highest total volume (typically the front contract), and
False for all others in the series.

Usage:
  python compute_front_contract_questdb.py [--date YYYY-MM-DD] [--symbol SYMBOL]

Arguments:
  --date    Start date in YYYY-MM-DD format. If not provided, will prompt for input
            or start from the earliest date in the database.
  --symbol  Symbol to start processing from. If not provided, will prompt for input
            or process all symbols. Will process this symbol and all subsequent symbols.

Examples:
  python compute_front_contract_questdb.py
  python compute_front_contract_questdb.py --date 2024-01-15
  python compute_front_contract_questdb.py --symbol ESM4
  python compute_front_contract_questdb.py --date 2024-01-15 --symbol ESM4

Requirements:
  - QuestDB running and accessible
  - Environment variables set in .env file (see .env.example)
  - 'trades' table with columns: symbol, time, volume, symbol_period
"""

import os
import sys
import argparse
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv('qdb.env')

# Get the table name from environment variables, defaulting to 'trades' if not set
TABLE_NAME = os.getenv("DB_TABLE", "trades")

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

def get_symbols(cursor):
    """Fetches all distinct symbols from the database table."""
    try:
        cursor.execute(f"SELECT DISTINCT symbol FROM {TABLE_NAME};")
        symbols = [row[0] for row in cursor.fetchall()]
        print(f"Found symbols: {symbols}")
        return symbols
    except psycopg2.Error as e:
        print(f"Error fetching symbols: {e}", file=sys.stderr)
        return []

def get_start_date(cursor):
    """Fetches the earliest date from the database table."""
    try:
        cursor.execute(f"SELECT min(time) FROM {TABLE_NAME};")
        result = cursor.fetchone()
        start_date = result[0] if result and result[0] else datetime.now()
        print(f"Earliest record in database is from: {start_date.strftime('%Y-%m-%d')}")
        return start_date
    except psycopg2.Error as e:
        print(f"Error fetching start date: {e}", file=sys.stderr)
        return datetime.now()

def ensure_front_contract_column_exists(cursor):
    """Ensures the 'front_contract' column exists in the table."""
    try:
        # Check if the column exists using QuestDB's table_columns() function
        cursor.execute(f"""
            SELECT * 
            FROM table_columns('{TABLE_NAME}')
            WHERE column = 'front_contract';
        """)
        result = cursor.fetchone()

        if not result:
            # Column doesn't exist, add it
            print(f"Adding 'front_contract' column to '{TABLE_NAME}' table...")
            cursor.execute(f"""
                ALTER TABLE {TABLE_NAME} ADD COLUMN front_contract BOOLEAN;
            """)
            print(f"Added 'front_contract' column to '{TABLE_NAME}' table.")
        else:
            print(f"'front_contract' column already exists in '{TABLE_NAME}' table.")
    except psycopg2.Error as e:
        print(f"Error ensuring 'front_contract' column exists: {e}", file=sys.stderr)
        cursor.connection.rollback()

def deduplicate_data(cursor, symbol, date_str):
    """
    For a given symbol and day, sets front_contract to True for the symbol_period with the highest volume
    and False for the others.
    """
    try:
        # Calculate start and end timestamps for the day to use index-based filtering
        start_ts = datetime.strptime(date_str, "%Y-%m-%d")
        end_ts = start_ts + timedelta(days=1)
        start_ts_str = start_ts.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        end_ts_str = end_ts.strftime("%Y-%m-%dT%H:%M:%S.000000Z")

        # Get all symbol_periods for the given symbol and day, ordered by total volume
        cursor.execute(f"""
            SELECT symbol_period, sum(volume) as total_volume
            FROM {TABLE_NAME}
            WHERE symbol = %s AND time >= %s AND time < %s
            GROUP BY symbol_period
            ORDER BY total_volume DESC;
        """, (symbol, start_ts_str, end_ts_str))

        results = cursor.fetchall()

        if len(results) <= 1:
            print(f"  - No overlapping data for symbol '{symbol}' on {date_str}. Skipping.")
            return

        # The first result is the one to set as front contract
        symbol_period_to_keep = results[0][0]
        print(f"  - Setting front_contract=True for symbol_period '{symbol_period_to_keep}' with volume {results[0][1]}.")

        # First, set all front_contract to False for this symbol and day
        cursor.execute(f"""
            UPDATE {TABLE_NAME}
            SET front_contract = FALSE
            WHERE symbol = %s AND time >= %s AND time < %s;
        """, (symbol, start_ts_str, end_ts_str))

        # Then, set front_contract to True for the highest volume symbol_period
        cursor.execute(f"""
            UPDATE {TABLE_NAME}
            SET front_contract = TRUE
            WHERE symbol = %s AND time >= %s AND time < %s AND symbol_period = %s;
        """, (symbol, start_ts_str, end_ts_str, symbol_period_to_keep))

        # Get all other symbol_periods to set as not front contract
        symbol_periods_to_set_false = [row[0] for row in results[1:]]

        for sp_to_set_false in symbol_periods_to_set_false:
            print(f"  - Setting front_contract=False for symbol_period '{sp_to_set_false}'.")

    except psycopg2.Error as e:
        print(f"  - Error processing symbol '{symbol}' on {date_str}: {e}", file=sys.stderr)
        cursor.connection.rollback()


def compute_average_daily_volume(symbol, cursor):
    """
    Computes the average daily volume for the latest 30 days where front_contract = true
    for the given symbol.
    """
    try:
        # Get the latest 30 distinct dates with data for this symbol
        cursor.execute(f"""
            SELECT DISTINCT to_str(time, 'yyyy-MM-dd') as date
            FROM {TABLE_NAME}
            WHERE symbol = %s AND front_contract = true
            ORDER BY date DESC
            LIMIT 30;
        """, (symbol,))

        recent_dates = cursor.fetchall()

        if not recent_dates:
            print(f"  - No recent data found for symbol '{symbol}'. Skipping average volume computation.")
            return 0

        # For each of these dates, sum the volume where front_contract = true
        daily_volumes = []
        for (date_str,) in recent_dates:
            start_ts = datetime.strptime(date_str, "%Y-%m-%d")
            end_ts = start_ts + timedelta(days=1)
            start_ts_str = start_ts.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
            end_ts_str = end_ts.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
            
            cursor.execute(f"""
                SELECT sum(volume) as daily_volume
                FROM {TABLE_NAME}
                WHERE symbol = %s AND time >= %s AND time < %s AND front_contract = true;
            """, (symbol, start_ts_str, end_ts_str))

            result = cursor.fetchone()
            if result and result[0]:
                daily_volumes.append(result[0])

        if not daily_volumes:
            print(f"  - No volume data found for symbol '{symbol}' in recent dates. Skipping average volume computation.")
            return 0

        avg_volume = sum(daily_volumes) / len(daily_volumes)
        print(f"  - Computed average daily volume for '{symbol}': {avg_volume:.2f} (based on {len(daily_volumes)} days)")
        return avg_volume

    except psycopg2.Error as e:
        print(f"  - Error computing average daily volume for symbol '{symbol}': {e}", file=sys.stderr)
        return 0


def drop_lowvolume_days(symbol, start_date, end_date, avg_volume, cursor):
    """
    Identifies and removes low volume days (holidays) for a symbol by setting front_contract = false
    for days where the volume is below 20% of the average daily volume.
    """
    if avg_volume <= 0:
        print(f"  - Invalid average volume for symbol '{symbol}'. Skipping low volume day detection.")
        return

    threshold = avg_volume * 0.20  # 20% of average volume
    print(f"  - Low volume threshold for '{symbol}': {threshold:.2f} (20% of average {avg_volume:.2f})")

    current_date = start_date
    low_volume_days = []

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")

        try:
            start_ts = current_date
            end_ts = start_ts + timedelta(days=1)
            start_ts_str = start_ts.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
            end_ts_str = end_ts.strftime("%Y-%m-%dT%H:%M:%S.000000Z")

            # Get the total volume for this day where front_contract = true
            cursor.execute(f"""
                SELECT sum(volume) as daily_volume
                FROM {TABLE_NAME}
                WHERE symbol = %s AND time >= %s AND time < %s AND front_contract = true;
            """, (symbol, start_ts_str, end_ts_str))

            result = cursor.fetchone()
            daily_volume = result[0] if result and result[0] else 0

            if daily_volume < threshold and daily_volume > 0:
                print(f"  - Low volume day detected: {date_str} (volume: {daily_volume:.2f} < threshold: {threshold:.2f})")
                low_volume_days.append(date_str)

                # Set front_contract = false for all records on this day for this symbol
                cursor.execute(f"""
                    UPDATE {TABLE_NAME}
                    SET front_contract = FALSE
                    WHERE symbol = %s AND time >= %s AND time < %s;
                """, (symbol, start_ts_str, end_ts_str))
            elif daily_volume > 0:
                print(f"  - Normal volume day: {date_str} (volume: {daily_volume:.2f})")

        except psycopg2.Error as e:
            print(f"  - Error processing low volume detection for symbol '{symbol}' on {date_str}: {e}", file=sys.stderr)

        current_date += timedelta(days=1)

    if low_volume_days:
        print(f"  - Removed {len(low_volume_days)} low volume days for symbol '{symbol}': {', '.join(low_volume_days)}")
    else:
        print(f"  - No low volume days detected for symbol '{symbol}'")


def main():
    """Main function to run the deduplication process."""
    parser = argparse.ArgumentParser(
        description="Deduplicate trading data in QuestDB by marking front contracts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            %(prog)s
            %(prog)s --date 2024-01-15
            %(prog)s --symbol AAPL
            %(prog)s --date 2024-01-15 --symbol AAPL
        """
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Start date in YYYY-MM-DD format. If not provided, will prompt for input or start from the earliest date in the database."
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="Symbol to start processing from. If not provided, will prompt for input or process all symbols."
    )

    args = parser.parse_args()

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Ensure the front_contract column exists
            ensure_front_contract_column_exists(cursor)
            conn.commit()

            # Get start date from command line argument or prompt
            start_date = None
            if args.date:
                try:
                    start_date = datetime.strptime(args.date, "%Y-%m-%d")
                except ValueError:
                    print(f"Invalid date format: {args.date}. Please use YYYY-MM-DD.", file=sys.stderr)
                    return
            else:
                user_start_date_str = input(f"Enter the start date (YYYY-MM-DD), or press Enter to start from the beginning: ")
                if user_start_date_str:
                    try:
                        start_date = datetime.strptime(user_start_date_str, "%Y-%m-%d")
                    except ValueError:
                        print("Invalid date format. Please use YYYY-MM-DD.", file=sys.stderr)
                        return
                else:
                    start_date = get_start_date(cursor)

            # Get start symbol from command line argument or prompt
            user_start_symbol = args.symbol.upper() if args.symbol else None
            if not user_start_symbol:
                user_start_symbol = input("Enter the symbol to start with, or press Enter for all symbols: ").upper()

            all_symbols = get_symbols(cursor)
            if not all_symbols:
                print("No symbols found in the database.")
                return

            symbols_to_process = all_symbols
            if user_start_symbol:
                if user_start_symbol in all_symbols:
                    # Process from the specified symbol onwards
                    start_index = all_symbols.index(user_start_symbol)
                    symbols_to_process = all_symbols[start_index:]
                else:
                    print(f"Symbol '{user_start_symbol}' not found. Processing all symbols.")

            end_date = datetime.now()
            current_date = start_date
            actual_start_date = start_date

            print(f"\nStarting deduplication from {current_date.strftime('%Y-%m-%d')} for symbols: {symbols_to_process}")

            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                print(f"\nProcessing date: {date_str}")

                for symbol in symbols_to_process:
                    print(f"  Processing symbol: '{symbol}'")
                    deduplicate_data(cursor, symbol, date_str)

                # Commit changes for the current day
                conn.commit()
                print(f"\nCommitted changes for {date_str}")

                current_date += timedelta(days=1)
                # After the first day, all symbols should be processed
                symbols_to_process = all_symbols

            # After main processing, perform low volume day removal
            print("\nStarting low volume day detection and removal...")

            for symbol in all_symbols:
                print(f"\nProcessing low volume detection for symbol: '{symbol}'")

                # Compute average daily volume for the symbol
                avg_volume = compute_average_daily_volume(symbol, cursor)

                if avg_volume > 0:
                    # Drop low volume days for the symbol
                    # DISABLING as it is causing high CPU load on QuestDB server
                    #drop_lowvolume_days(symbol, actual_start_date, end_date, avg_volume, cursor)
                    continue

                # Commit changes for this symbol
                conn.commit()
                print(f"Committed low volume changes for symbol '{symbol}'")

            print("\nDeduplication and low volume day removal process completed.")

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
