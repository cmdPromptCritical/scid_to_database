## Current state: This script reads data from a SCID file, processes it, and loads it into a QuestDB database using parallel batch processing. It handles checkpoints to ensure data continuity and allows for periodic updates.
## It assumes the following QuestDB table schema:
## CREATE TABLE trades (
##     time TIMESTAMP,                -- Designated timestamp for time-series queries
##     open DOUBLE,                   -- Use DOUBLE for performance (see note above)
##     high DOUBLE,
##     low DOUBLE,
##     close DOUBLE,
##     volume INT,
##     number_of_trades INT,
##     bid_volume INT,
##     ask_volume INT,
##     symbol SYMBOL CAPACITY 256,
##     symbol_period SYMBOL CAPACITY 256
##     front_contract BOOLEAN, -- to mark the front contract. set to False for all historical data and another process will set it to True for the current front contract
## ) TIMESTAMP(time)
## PARTITION BY DAY WAL
## DEDUP UPSERT KEYS(time, symbol, symbol_period);

import asyncio
import polars as pl
import pandas as pd
import numpy as np
import sys
from pathlib import Path
import time
import os
import json
from dotenv import load_dotenv
import re
import queue
import signal
from concurrent.futures import ThreadPoolExecutor
from questdb.ingress import Sender, IngressError, TimestampNanos
import psycopg2

class WorkerFailureException(Exception):
    """Exception raised when one or more workers fail during batch processing"""
    pass

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    print(f"\nReceived signal {signum}. Initiating graceful shutdown...")
    shutdown_requested = True

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# Load environment variables from .env file
load_dotenv('qdb.env')

def create_table_if_not_exists(table_name, questdb_host, questdb_pg_port, user, password):
    """Create a table in QuestDB if it does not already exist."""
    conn_str = f"host='{questdb_host}' port='{questdb_pg_port}' dbname='qdb' user='{user}' password='{password}'"
    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    time TIMESTAMP,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume INT,
                    number_of_trades INT,
                    bid_volume INT,
                    ask_volume INT,
                    symbol SYMBOL CAPACITY 256,
                    symbol_period SYMBOL CAPACITY 256,
                    front_contract BOOLEAN
                ) TIMESTAMP(time)
                PARTITION BY DAY WAL
                DEDUP UPSERT KEYS(time, symbol, symbol_period);
                """
                cur.execute(create_table_query)
                print(f"Table '{table_name}' created or already exists.")
    except psycopg2.Error as e:
        print(f"Error connecting to QuestDB or creating table: {e}")
        sys.exit(1)

def get_scid_np(scidFile, offset=0, max_records=None):
    f = Path(scidFile)
    assert f.exists(), "SCID file not found"
    with open(scidFile, 'rb') as file:
        file.seek(0, os.SEEK_END)
        file_size = file.tell()  # Total size of the file
        sciddtype = np.dtype([
            ("scdatetime", "<u8"),
            ("open", "<f4"),
            ("high", "<f4"),
            ("low", "<f4"),
            ("close", "<f4"),
            ("numtrades", "<u4"),
            ("totalvolume", "<u4"),
            ("bidvolume", "<u4"),
            ("askvolume", "<u4"),
        ])
        record_size = sciddtype.itemsize

        # Adjust the offset if not within the file size
        if offset >= file_size:
            return np.array([]), offset # Return empty array if offset is beyond file size
        elif offset < 56:
            offset = 56  # Skip header assumed to be 56 bytes

        file.seek(offset)
        
        # Read either max_records or until the end of the file
        scid_as_np_array = np.fromfile(file, dtype=sciddtype, count=max_records if max_records is not None else -1)
        new_position = file.tell()  # Update the position after reading

    return scid_as_np_array, new_position

def send_batch(conf_str, table_name, batches, timestamp_name, df_processed_polars, worker_id, file_name, total_batches):
    """
    Worker function to process batches of data from a shared queue and send them to QuestDB.
    This function is designed to be memory-efficient by converting only small batches to Pandas DataFrames.
    """
    try:
        with Sender.from_conf(conf_str, auto_flush=False, init_buf_size=100_000_000) as qdb_sender:
            batch_count = 0
            while not shutdown_requested:
                try:
                    start_idx, end_idx = batches.get(timeout=1)
                    batch_count += 1
                    progress = (batch_count / total_batches) * 100 if total_batches > 0 else 0
                    print(f"Worker {worker_id} [{file_name}]: Processing batch {batch_count}/{total_batches} ({progress:.1f}%) - rows {start_idx}-{end_idx}")

                    # Slice the Polars DataFrame (zero-copy)
                    batch_df_polars = df_processed_polars[start_idx:end_idx]

                    # Convert only the small slice to Pandas
                    batch_df_pandas = batch_df_polars.to_pandas()

                    # Ensure timestamp column is correctly formatted
                    batch_df_pandas['time'] = pd.to_datetime(batch_df_pandas['time'], utc=True, unit='us')

                    # Send the batch to QuestDB
                    qdb_sender.dataframe(
                        batch_df_pandas,
                        table_name=table_name,
                        symbols=['symbol', 'symbol_period'],
                        at=timestamp_name
                    )
                    qdb_sender.flush()
                    print(f"Worker {worker_id} [{file_name}]: Successfully sent batch {batch_count}")

                    # Explicitly delete the batch to free memory
                    del batch_df_polars
                    del batch_df_pandas

                    # Mark task as done
                    batches.task_done()

                except queue.Empty:
                    # No more batches to process in the queue
                    break
                except Exception as e:
                    print(f"Worker {worker_id} [{file_name}]: Error processing batch {batch_count} at rows {start_idx}-{end_idx}: {e}")
                    batches.task_done()
                    # Re-raise the exception to be caught by the main thread
                    raise

            if shutdown_requested:
                print(f"Worker {worker_id} [{file_name}]: Shutdown requested, exiting gracefully")
            else:
                print(f"Worker {worker_id} [{file_name}]: Completed. Processed {batch_count} batches.")

    except IngressError as e:
        print(f"Worker {worker_id} [{file_name}]: QuestDB ingestion error: {e}")
        raise
    except Exception as e:
        # Catch errors from both inside the loop and from the Sender context manager
        print(f"Worker {worker_id} [{file_name}]: An unexpected error occurred: {e}")
        raise

def load_data_to_questdb(df, table_name, symbol, symbol_period, file_name, questdb_host='localhost', questdb_port=9009):
    """Load data into QuestDB using parallel batch processing in a memory-efficient way."""
    
    # SCDateTime epoch is December 30, 1899
    epoch = pl.datetime(1899, 12, 30, 0, 0, 0, time_unit="us")

    # Process the dataframe to match QuestDB schema. This remains a Polars DataFrame.
    df_processed = df.with_columns([
        (epoch + pl.duration(microseconds=pl.col('scdatetime'))).alias('time'),
        pl.col('open').cast(pl.Float64),
        pl.col('high').cast(pl.Float64),
        pl.col('low').cast(pl.Float64),
        pl.col('close').cast(pl.Float64),
        pl.col('totalvolume').alias('volume').cast(pl.Int32),
        pl.col('numtrades').alias('number_of_trades').cast(pl.Int32),
        pl.col('bidvolume').alias('bid_volume').cast(pl.Int32),
        pl.col('askvolume').alias('ask_volume').cast(pl.Int32),
        pl.lit(symbol).alias('symbol'),
        pl.lit(symbol_period).alias('symbol_period'),
        pl.lit(False).alias('front_contract')
    ]).select([
        'time', 'open', 'high', 'low', 'close',
        'volume', 'number_of_trades', 'bid_volume', 'ask_volume', 'symbol', 'symbol_period', 'front_contract'
    ])

    # Convert time column to int64 for QuestDB. Still a Polars operation.
    df_processed = df_processed.with_columns(
        pl.col('time').cast(pl.Int64).alias('time')
    )
    
    print(f"[{file_name}]: Preparing to load {len(df_processed)} records to QuestDB")

    # Create batches of indices using thread-safe queue
    batches = queue.Queue()
    batch_size = int(os.getenv("BATCH_SIZE", "200000"))
    parallel_workers = int(os.getenv("PARALLEL_WORKERS", "8"))

    total_rows = len(df_processed)
    total_batches = total_rows // batch_size + (1 if total_rows % batch_size > 0 else 0)
    print(f"[{file_name}]: Splitting data into {total_batches} batches of up to {batch_size} records each")

    for i in range(total_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_rows)
        if start_idx < end_idx:
            batches.put((start_idx, end_idx))

    # QuestDB connection configuration
    conf_str = f'http::addr={questdb_host}:{questdb_port};'
    timestamp_name = 'time'

    print(f"[{file_name}]: Starting parallel ingestion with {parallel_workers} workers")
    start_time = time.time()

    # Use ThreadPoolExecutor for parallel batch processing
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        # Pass the Polars DataFrame to the workers. This is a reference, not a copy.
        futures = [
            executor.submit(send_batch, conf_str, table_name, batches, timestamp_name, df_processed, i + 1, file_name, total_batches)
            for i in range(parallel_workers)
        ]
        
        # Wait for all workers to complete and check for failures
        worker_failed = False
        for i, future in enumerate(futures):
            try:
                future.result()  # This will re-raise any exception from the worker
                print(f"[{file_name}]: Worker {i+1} finished its tasks successfully.")
            except Exception as e:
                print(f"[{file_name}]: Worker {i+1} failed with a critical error: {e}")
                worker_failed = True

        if worker_failed:
            # Cancel any remaining futures to stop processing immediately
            for f in futures:
                f.cancel()
            raise WorkerFailureException(f"One or more workers failed while processing {file_name}. Halting processing for this chunk.")

    end_time = time.time()
    print(f"[{file_name}]: Batch processing completed in {end_time - start_time:.2f} seconds")
    
    if batches.empty():
        print(f"[{file_name}]: All batches processed successfully")
    else:
        remaining = batches.qsize()
        print(f"[{file_name}]: Warning: {remaining} batches remaining unprocessed due to an early exit.")

def main(table_name, scid_file):
    """Main processing function"""
    start_time = time.time()
    file_name = Path(scid_file).name

    # Get QuestDB connection details from environment variables for table creation
    questdb_host = os.getenv("DB_HOST", "localhost")
    questdb_pg_port = int(os.getenv("QUESTDB_PG_PORT", "8812"))
    questdb_user = os.getenv("DB_USER", "admin")
    questdb_password = os.getenv("DB_PASSWORD", "quest")
    questdb_port = int(os.getenv("DB_PORT", "9000"))
    
    # Define how many records to process in one memory-resident chunk
    processing_chunk_size = int(os.getenv("PROCESSING_CHUNK_SIZE", "2000000"))
    max_retries = int(os.getenv("MAX_CHUNK_RETRIES", "3"))

    # Create table if it doesn't exist
    create_table_if_not_exists(table_name, questdb_host, questdb_pg_port, questdb_user, questdb_password)

    # --- Symbol and Period Parsing ---
    file_stem = Path(scid_file).stem
    # Pattern matches: SYMBOL + (H/M/U/Z) + NUMBER(S) + . + EXCHANGE
    # Examples: MGCM5.COMEX, MESH5.CME, ESH26.cme
    # Captures: symbol and symbol_period (e.g., MGC and M5, MES and H5, ES and H26)
    pattern = r'^([A-Z]+)([HMUZ]\d+)\.(.+)

    # --- Checkpoint Loading ---
    checkpoint_file = Path(f"checkpoint_qdb.json")
    checkpoint_key = f'{symbol}{symbol_period}'
    last_position = 0
    checkpoint_data = {}

    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, "r") as f:
                checkpoint_data = json.load(f)
                table_data = checkpoint_data.get(checkpoint_key, {})
                last_position = table_data.get("last_position", 0)
                print(f"[{file_name}]: Resuming from position {last_position} for {checkpoint_key}.")
        except (json.JSONDecodeError, IOError) as e:
            print(f"[{file_name}]: Checkpoint file '{checkpoint_file}' is corrupted or unreadable: {e}. Starting fresh.")
            checkpoint_data = {}
    
    print(f"[{file_name}]: Processing SCID file: {scid_file}, Symbol: {symbol}, Period: {symbol_period}")

    # --- Chunked File Processing Loop ---
    all_chunks_processed = False
    chunk_number = 0
    while not shutdown_requested:
        try:
            chunk_number += 1
            intermediate_np_array, new_position = get_scid_np(scid_file, offset=last_position, max_records=processing_chunk_size)

            if intermediate_np_array.size == 0:
                print(f"[{file_name}]: No new data found for {checkpoint_key} at position {last_position}.")
                all_chunks_processed = True
                break  # Exit loop when no more records are read

            print(f"[{file_name}]: Chunk {chunk_number} - Read {len(intermediate_np_array)} new records from position {last_position}.")
            df_raw = pl.DataFrame(intermediate_np_array)
            del intermediate_np_array # Free memory
            
            # Retry logic for failed chunks
            retry_count = 0
            chunk_success = False
            while retry_count < max_retries and not shutdown_requested:
                try:
                    load_data_to_questdb(df_raw, table_name, symbol, symbol_period, file_name, questdb_host, questdb_port)
                    chunk_success = True
                    break
                except WorkerFailureException as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"[{file_name}]: Chunk {chunk_number} failed (attempt {retry_count}/{max_retries}). Retrying in 5 seconds...")
                        time.sleep(5)
                    else:
                        print(f"[{file_name}]: Chunk {chunk_number} failed after {max_retries} attempts. Moving to next chunk.")
                        raise
            
            if not chunk_success:
                print(f"[{file_name}]: Chunk {chunk_number} could not be processed after {max_retries} attempts.")
                raise WorkerFailureException(f"Failed to process chunk {chunk_number} after {max_retries} retries")
            
            # --- Checkpoint Saving ---
            last_position = new_position
            checkpoint_data[checkpoint_key] = {
                "last_position": last_position,
                "initial_load_done": False # Mark as not done until the very end
            }
            with open(checkpoint_file, "w") as f:
                json.dump(checkpoint_data, f, indent=4)
            print(f"[{file_name}]: Checkpoint updated to position {last_position} for {checkpoint_key}.")
        
        except WorkerFailureException as e:
            print(f"[{file_name}]: CHUNK {chunk_number} FAILED: {e}. Current progress for this chunk is lost. Checkpoint is at {last_position}.")
            print(f"[{file_name}]: The script will retry from the last successful checkpoint in the next cycle.")
            raise # Re-raise to be caught by the main execution loop

        except Exception as e:
            print(f"[{file_name}]: An unexpected error occurred during chunk {chunk_number} processing at position {last_position}: {e}")
            raise # Re-raise to be caught by the main execution loop

    # --- Finalize Checkpoint ---
    if all_chunks_processed and not shutdown_requested:
        table_data = checkpoint_data.get(checkpoint_key, {})
        table_data['initial_load_done'] = True
        table_data['last_position'] = last_position # Ensure last position is current
        checkpoint_data[checkpoint_key] = table_data
        
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint_data, f, indent=4)
        print(f"[{file_name}]: Finished processing all data for {checkpoint_key}. Final checkpoint saved.")
    elif shutdown_requested:
        print(f"[{file_name}]: Shutdown requested during processing. Last checkpoint at position {last_position}.")

    end_time = time.time()
    print(f"[{file_name}]: Total execution time for this run: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    table_name = os.getenv("DB_TABLE", "trades")  # QuestDB table name
    scid_folder_path = os.getenv("SCID_FOLDER", r"C:\auxDrive\SierraChart2\Data") # Default data folder

    # Continuously update data from SCID files found in the folder
    while not shutdown_requested:
        try:
            scid_folder = Path(scid_folder_path)
            if not scid_folder.is_dir():
                print(f"Error: The path specified in SCID_FOLDER is not a valid directory: '{scid_folder_path}'")
                break  # Exit if the directory is invalid

            files_to_process = list(scid_folder.glob('*.scid'))
            print(f"Found {len(files_to_process)} .scid files to process in '{scid_folder_path}'.")

            # Loop through each file and process it
            for scid_file in files_to_process:
                if shutdown_requested:
                    print("Shutdown requested, stopping file processing.")
                    break
                
                print(f"\n--- Starting processing for {scid_file.name} ---")
                try:
                    # The main function handles the entire logic for one file
                    main(table_name, str(scid_file))
                except WorkerFailureException as e:
                    print(f"A worker failed while processing {scid_file.name}: {e}. Continuing to the next file.")
                except Exception as e:
                    print(f"An unexpected error occurred processing {scid_file.name}: {e}. Continuing to the next file.")

            if shutdown_requested:
                print("Shutdown requested, exiting main loop.")
                break

            sleep_duration = int(os.getenv("SLEEP_DURATION", "3600"))  # Default to 1 hour
            print(f"\n--- Cycle complete. Sleeping for {sleep_duration} seconds... ---")
            
            # Sleep in small intervals to allow for graceful shutdown
            for _ in range(sleep_duration):
                if shutdown_requested:
                    print("Shutdown requested during sleep, exiting.")
                    break
                time.sleep(1)

        except KeyboardInterrupt:
            print("Process interrupted by user. Exiting.")
            shutdown_requested = True
            break
        except Exception as e:
            # This catches broader errors like the directory not existing at startup
            print(f"An unexpected error occurred in the main loop: {e}")
            if not shutdown_requested:
                print("Retrying in 60 seconds...")
                time.sleep(60)
    
    print("Script terminated gracefully.")

    match = re.match(pattern, file_stem, re.IGNORECASE)
    if match:
        symbol = match.group(1).upper()
        symbol_period = match.group(2).upper()
        exchange = match.group(3).upper()
        print(f"[{file_name}]: Parsed - Symbol: {symbol}, Period: {symbol_period}, Exchange: {exchange}")
    else:
        print(f"[{file_name}]: File name does not match expected pattern (SYMBOL[HMUZ]NUMBER.EXCHANGE.scid). Skipping file.")
        return  # Skip this file entirely

    # --- Checkpoint Loading ---
    checkpoint_file = Path(f"checkpoint_qdb.json")
    checkpoint_key = f'{symbol}{symbol_period}'
    last_position = 0
    checkpoint_data = {}

    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, "r") as f:
                checkpoint_data = json.load(f)
                table_data = checkpoint_data.get(checkpoint_key, {})
                last_position = table_data.get("last_position", 0)
                print(f"[{file_name}]: Resuming from position {last_position} for {checkpoint_key}.")
        except (json.JSONDecodeError, IOError) as e:
            print(f"[{file_name}]: Checkpoint file '{checkpoint_file}' is corrupted or unreadable: {e}. Starting fresh.")
            checkpoint_data = {}
    
    print(f"[{file_name}]: Processing SCID file: {scid_file}, Symbol: {symbol}, Period: {symbol_period}")

    # --- Chunked File Processing Loop ---
    all_chunks_processed = False
    chunk_number = 0
    while not shutdown_requested:
        try:
            chunk_number += 1
            intermediate_np_array, new_position = get_scid_np(scid_file, offset=last_position, max_records=processing_chunk_size)

            if intermediate_np_array.size == 0:
                print(f"[{file_name}]: No new data found for {checkpoint_key} at position {last_position}.")
                all_chunks_processed = True
                break  # Exit loop when no more records are read

            print(f"[{file_name}]: Chunk {chunk_number} - Read {len(intermediate_np_array)} new records from position {last_position}.")
            df_raw = pl.DataFrame(intermediate_np_array)
            del intermediate_np_array # Free memory
            
            # Retry logic for failed chunks
            retry_count = 0
            chunk_success = False
            while retry_count < max_retries and not shutdown_requested:
                try:
                    load_data_to_questdb(df_raw, table_name, symbol, symbol_period, file_name, questdb_host, questdb_port)
                    chunk_success = True
                    break
                except WorkerFailureException as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"[{file_name}]: Chunk {chunk_number} failed (attempt {retry_count}/{max_retries}). Retrying in 5 seconds...")
                        time.sleep(5)
                    else:
                        print(f"[{file_name}]: Chunk {chunk_number} failed after {max_retries} attempts. Moving to next chunk.")
                        raise
            
            if not chunk_success:
                print(f"[{file_name}]: Chunk {chunk_number} could not be processed after {max_retries} attempts.")
                raise WorkerFailureException(f"Failed to process chunk {chunk_number} after {max_retries} retries")
            
            # --- Checkpoint Saving ---
            last_position = new_position
            checkpoint_data[checkpoint_key] = {
                "last_position": last_position,
                "initial_load_done": False # Mark as not done until the very end
            }
            with open(checkpoint_file, "w") as f:
                json.dump(checkpoint_data, f, indent=4)
            print(f"[{file_name}]: Checkpoint updated to position {last_position} for {checkpoint_key}.")
        
        except WorkerFailureException as e:
            print(f"[{file_name}]: CHUNK {chunk_number} FAILED: {e}. Current progress for this chunk is lost. Checkpoint is at {last_position}.")
            print(f"[{file_name}]: The script will retry from the last successful checkpoint in the next cycle.")
            raise # Re-raise to be caught by the main execution loop

        except Exception as e:
            print(f"[{file_name}]: An unexpected error occurred during chunk {chunk_number} processing at position {last_position}: {e}")
            raise # Re-raise to be caught by the main execution loop

    # --- Finalize Checkpoint ---
    if all_chunks_processed and not shutdown_requested:
        table_data = checkpoint_data.get(checkpoint_key, {})
        table_data['initial_load_done'] = True
        table_data['last_position'] = last_position # Ensure last position is current
        checkpoint_data[checkpoint_key] = table_data
        
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint_data, f, indent=4)
        print(f"[{file_name}]: Finished processing all data for {checkpoint_key}. Final checkpoint saved.")
    elif shutdown_requested:
        print(f"[{file_name}]: Shutdown requested during processing. Last checkpoint at position {last_position}.")

    end_time = time.time()
    print(f"[{file_name}]: Total execution time for this run: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    table_name = os.getenv("DB_TABLE", "trades")  # QuestDB table name
    scid_folder_path = os.getenv("SCID_FOLDER", r"C:\auxDrive\SierraChart2\Data") # Default data folder

    # Continuously update data from SCID files found in the folder
    while not shutdown_requested:
        try:
            scid_folder = Path(scid_folder_path)
            if not scid_folder.is_dir():
                print(f"Error: The path specified in SCID_FOLDER is not a valid directory: '{scid_folder_path}'")
                break  # Exit if the directory is invalid

            files_to_process = list(scid_folder.glob('*.scid'))
            print(f"Found {len(files_to_process)} .scid files to process in '{scid_folder_path}'.")

            # Loop through each file and process it
            for scid_file in files_to_process:
                if shutdown_requested:
                    print("Shutdown requested, stopping file processing.")
                    break
                
                print(f"\n--- Starting processing for {scid_file.name} ---")
                try:
                    # The main function handles the entire logic for one file
                    main(table_name, str(scid_file))
                except WorkerFailureException as e:
                    print(f"A worker failed while processing {scid_file.name}: {e}. Continuing to the next file.")
                except Exception as e:
                    print(f"An unexpected error occurred processing {scid_file.name}: {e}. Continuing to the next file.")

            if shutdown_requested:
                print("Shutdown requested, exiting main loop.")
                break

            sleep_duration = int(os.getenv("SLEEP_DURATION", "3600"))  # Default to 1 hour
            print(f"\n--- Cycle complete. Sleeping for {sleep_duration} seconds... ---")
            
            # Sleep in small intervals to allow for graceful shutdown
            for _ in range(sleep_duration):
                if shutdown_requested:
                    print("Shutdown requested during sleep, exiting.")
                    break
                time.sleep(1)

        except KeyboardInterrupt:
            print("Process interrupted by user. Exiting.")
            shutdown_requested = True
            break
        except Exception as e:
            # This catches broader errors like the directory not existing at startup
            print(f"An unexpected error occurred in the main loop: {e}")
            if not shutdown_requested:
                print("Retrying in 60 seconds...")
                time.sleep(60)
    
    print("Script terminated gracefully.")