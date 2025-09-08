import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv
import os
load_dotenv()


conn= psycopg2.connect(
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database=os.getenv('DB_NAME'))



def pivot_crypto_data():
    # Load raw data
    df = pd.read_sql("SELECT * FROM crypto_prices_raw", conn)
    # Convert and floor timestamps to the nearest hour
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.floor('H')

    # Group by floored timestamp and coin, then compute the mean price
    grouped = df.groupby(['timestamp', 'coin'])['price'].mean().reset_index()

    # Pivot the grouped DataFrame
    pivot_df = grouped.pivot(index='timestamp', columns='coin', values='price')
    pivot_df.columns.name = None
    pivot_df = pivot_df.dropna().reset_index()

    # Load existing timestamps in the crypto_prices table
    try:
        existing = pd.read_sql("SELECT timestamp FROM crypto_prices", conn)
        existing_timestamps = set(existing['timestamp'])
    except Exception:
        # Table doesn't exist yet
        existing_timestamps = set()

    # Filter only new rows
    new_rows = pivot_df[~pivot_df['timestamp'].isin(existing_timestamps)]

    if not new_rows.empty:
        new_rows.to_sql('crypto_prices', conn, if_exists='append', index=False)
        print(f"Inserted {len(new_rows)} new rows into crypto_prices.")
    else:
        print("No new data to insert.")

