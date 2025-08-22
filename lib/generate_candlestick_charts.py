#!/usr/bin/env python3
"""
Candlestick Chart Generator for Stock Data

This script generates daily candlestick charts from QuestDB trading data.
It iterates through each trading day from a specified start date, queries
the database for OHLC data, and creates candlestick charts using matplotlib and mplfinance.

Features:
- Command line interface for symbol and start date selection
- Automatic iteration through trading days
- Candlestick charts with proper time formatting
- Automatic chart saving to 'charts' subdirectory
- Error handling and logging

Usage:
    python generate_candlestick_charts.py --symbol SYMBOL --start-date YYYY-MM-DD

Requirements:
    - QuestDB running and accessible
    - qdb.env file with database connection parameters
    - matplotlib library installed
    - mplfinance library installed
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
import matplotlib.pyplot as plt
import mplfinance as mpf

# Load environment variables from qdb.env file
env_path = Path(__file__).parent / 'qdb.env'
load_dotenv(env_path)

# Global variable to store the current figure for saving
current_figure = None

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate candlestick charts from QuestDB trading data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_candlestick_charts.py --symbol AAPL --start-date 2024-01-01
  python generate_candlestick_charts.py --symbol GOOGL --start-date 2024-06-01
        """
    )

    parser.add_argument(
        '--symbol',
        required=True,
        help='Stock symbol to generate charts for (e.g., AAPL, GOOGL)'
    )

    parser.add_argument(
        '--start-date',
        required=True,
        help='Start date in YYYY-MM-DD format'
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
        print("Successfully connected to QuestDB.")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to QuestDB: {e}", file=sys.stderr)
        sys.exit(1)

def get_trading_data_for_date(conn, symbol, target_date):
    """Query trading data for a specific symbol and date."""

    target_date_plus_one = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    query = f"""
    SELECT *
    FROM trades_ohlc_15s
    WHERE
        hour(to_timezone(time, 'America/New_York')) >= 9
        AND (
            hour(to_timezone(time, 'America/New_York')) > 9
            OR minute(to_timezone(time, 'America/New_York')) >= 30
        )
        AND hour(to_timezone(time, 'America/New_York')) < 16
        AND to_timezone(time, 'America/New_York')::date > '{target_date}'
        AND to_timezone(time, 'America/New_York')::date < '{target_date_plus_one}'
        AND symbol = '{symbol}'
    ORDER BY time
    """

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

        if not rows:
            print(f"No data found for {symbol} on {target_date}")
            return None

        df = pd.DataFrame(rows, columns=columns)
        # Convert time column to datetime if it's not already
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])

        return df

    except psycopg2.Error as e:
        print(f"Error querying data for {symbol} on {target_date}: {e}", file=sys.stderr)
        return None

def create_candlestick_chart(df, symbol, date):
    """Create and save candlestick chart for the given data."""
    if df is None or df.empty:
        print(f"No data to plot for {symbol} on {date}")
        return False

    try:
        # Prepare data for mplfinance - set time as index
        chart_data = df.set_index('time')[['open', 'high', 'low', 'close']]
        chart_data.index.name = 'Date'

        # Create style for the chart
        mc = mpf.make_marketcolors(up='green', down='red', inherit=True)
        s = mpf.make_mpf_style(marketcolors=mc)

        # Create the candlestick chart
        fig, ax = mpf.plot(
            chart_data,
            type='candle',
            style=s,
            volume=False,
            title=f'{symbol} Candlestick Chart - {date}',
            returnfig=True,
            figsize=(12, 8)
        )

        # Store the figure for later saving
        global current_figure
        current_figure = fig

        return True

    except Exception as e:
        print(f"Error creating chart for {symbol} on {date}: {e}", file=sys.stderr)
        return False

def save_chart(symbol, date):
    """Save the current chart to a file."""
    global current_figure

    try:
        # Ensure charts directory exists
        charts_dir = Path(__file__).parent / 'charts'
        charts_dir.mkdir(exist_ok=True)

        # Create filename with symbol and date
        filename = f"{symbol}_{date}.png"
        filepath = charts_dir / filename

        # Save the figure if it exists
        if current_figure is not None:
            current_figure.savefig(filepath, dpi=100, bbox_inches='tight')
            plt.close(current_figure)  # Close the figure to free memory
            current_figure = None  # Reset for next chart
            print(f"Chart saved: {filepath}")
            return True
        else:
            print(f"No figure to save for {symbol} on {date}")
            return False

    except Exception as e:
        print(f"Error saving chart for {symbol} on {date}: {e}", file=sys.stderr)
        return False

def generate_trading_dates(start_date_str):
    """Generate list of trading dates from start date to current date."""
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.now().date()

        trading_dates = []
        current_date = start_date

        while current_date <= end_date:
            # Skip weekends (Saturday = 5, Sunday = 6)
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                trading_dates.append(current_date)
            current_date += timedelta(days=1)

        return trading_dates

    except ValueError as e:
        print(f"Error parsing start date '{start_date_str}': {e}", file=sys.stderr)
        sys.exit(1)

def main():
    """Main function to generate candlestick charts."""
    print("Candlestick Chart Generator for Stock Data")
    print("=" * 50)

    # Parse command line arguments
    args = parse_arguments()

    # Validate and generate trading dates
    trading_dates = generate_trading_dates(args.start_date)

    if not trading_dates:
        print("No trading dates found in the specified range.")
        return

    print(f"Generating charts for {args.symbol} from {args.start_date} to {trading_dates[-1]}")
    print(f"Total trading days to process: {len(trading_dates)}")
    print("-" * 50)

    # Connect to database
    conn = get_db_connection()

    try:
        charts_generated = 0
        charts_skipped = 0

        for date in trading_dates:
            date_str = date.strftime('%Y-%m-%d')
            print(f"Processing {args.symbol} for {date_str}...")

            # Get trading data for this date
            df = get_trading_data_for_date(conn, args.symbol, date_str)

            if df is not None and not df.empty:
                # Create candlestick chart
                if create_candlestick_chart(df, args.symbol, date_str):
                    # Save the chart
                    if save_chart(args.symbol, date_str):
                        charts_generated += 1
                        print(f"✓ Chart generated successfully for {date_str}")
                    else:
                        charts_skipped += 1
                        print(f"✗ Failed to save chart for {date_str}")
                else:
                    charts_skipped += 1
                    print(f"✗ Failed to create chart for {date_str}")
            else:
                charts_skipped += 1
                print(f"✗ No data available for {date_str}")

        print("-" * 50)
        print(f"Processing complete!")
        print(f"Charts generated: {charts_generated}")
        print(f"Charts skipped: {charts_skipped}")
        print(f"Charts saved to: {Path(__file__).parent / 'charts'}")

    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
        sys.exit(1)

    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()