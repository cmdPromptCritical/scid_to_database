import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def get_db_connection():
    """Establishes a connection to the QuestDB database with a longer timeout."""
    load_dotenv('qdb.env')
    # Set a 5-minute statement timeout (in milliseconds) to handle long-running queries on a busy DB
    conn_str = "host='{}' port={} dbname='qdb' user='{}' password='{}' options='-c statement_timeout=300000'".format(
        os.getenv("DB_HOST", "localhost"),
        os.getenv("QUESTDB_PG_PORT", 8812),
        os.getenv("DB_USER", "admin"),
        os.getenv("DB_PASSWORD", "quest")
    )
    try:
        conn = psycopg2.connect(conn_str)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to QuestDB: {e}")
        return None

def fetch_daily_volume(conn):
    """
    Fetches the daily total trade volume for each symbol_period using
    QuestDB's SAMPLE BY clause for efficiency.
    """
    load_dotenv('qdb.env')
    table_name = os.getenv("DB_TABLE", "market_data")
    
    # This query efficiently samples the data by 1-day intervals,
    # counting trades for each symbol_period within those intervals.
    query = f"""
    SELECT time, symbol_period, count() as volume
    FROM {table_name}
    SAMPLE BY 1d;
    """
    
    try:
        print(f"Fetching daily volume from table: '{table_name}'...")
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print(f"No data returned from the query. Check the table '{table_name}'.")
            return None
        
        df['time'] = pd.to_datetime(df['time'])
        print("Successfully fetched daily volume data.")
        return df

    except (psycopg2.Error, pd.io.sql.DatabaseError) as e:
        print(f"Database query failed: {e}")
        return None

def plot_volume_distribution(df):
    """
    Generates and saves a stacked area chart of daily trade volumes for each
    base symbol, showing the transition of volume between contracts.
    """
    if df is None or df.empty:
        print("DataFrame is empty. Cannot generate plots.")
        return

    # Extract base symbol (e.g., 'ES' from 'ES_2025-03')
    df['symbol'] = df['symbol_period'].str.split('_').str[0]

    symbols = df['symbol'].unique()
    
    for symbol in symbols:
        print(f"Processing and plotting data for symbol: {symbol}...")
        
        symbol_df = df[df['symbol'] == symbol]
        
        # Pivot data to have time as index, symbol_periods as columns, and volume as values
        pivot_df = symbol_df.pivot(index='time', columns='symbol_period', values='volume').fillna(0)
        
        # Ensure columns are sorted chronologically (e.g., ES_2025-03, ES_2025-06)
        pivot_df = pivot_df.reindex(sorted(pivot_df.columns), axis=1)

        fig, ax = plt.subplots(figsize=(15, 7))
        
        # Generate the stacked area plot
        pivot_df.plot(
            kind='area',
            stacked=True,
            ax=ax,
            linewidth=0, # No lines between area segments for a cleaner look
            legend=True
        )

        # Formatting the plot
        ax.set_title(f'Daily Trading Volume Distribution for {symbol} Contracts', fontsize=16, pad=20)
        ax.set_ylabel('Number of Trades (Volume)', fontsize=12)
        ax.set_xlabel('Date', fontsize=12)
        ax.legend(title='Contract Period')
        
        # Improve date formatting on the x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=10, maxticks=20))
        fig.autofmt_xdate()
        
        ax.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()

        # Save the figure
        output_filename = f'{symbol}_volume_distribution.png'
        plt.savefig(output_filename)
        print(f"Plot for {symbol} saved successfully to '{output_filename}'")
        plt.close(fig) # Close the figure to free memory

if __name__ == "__main__":
    print("Connecting to database...")
    db_conn = get_db_connection()
    if db_conn:
        volume_df = fetch_daily_volume(db_conn)
        db_conn.close()
        
        if volume_df is not None:
            print("Generating plots...")
            plot_volume_distribution(volume_df)
        else:
            print("Exiting due to data fetch failure.")
    else:
        print("Exiting due to database connection failure.")