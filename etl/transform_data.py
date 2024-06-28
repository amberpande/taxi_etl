import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_column_name(df, possible_names):
    for name in possible_names:
        if name in df.columns:
            return name
    return None

def process_file(file_path, chunk_size=100000):
    for chunk in pq.ParquetFile(file_path).iter_batches(batch_size=chunk_size):
        yield chunk.to_pandas()

def transform_chunk(df):
    required_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'total_amount']
    
    for col in required_columns:
        if col not in df.columns:
            alt_col = get_column_name(df, [col, f'l{col[1:]}'])  # Check for lpep_ variants
            if alt_col:
                df[col] = df[alt_col]
            else:
                logger.warning(f"Column {col} not found in the dataset. This may cause issues in later processing.")
    
    return df

def run():
    start_time = datetime.now()
    logger.info("Starting data transformation process")

    daily_stats = pd.DataFrame()
    monthly_stats = pd.DataFrame()
    total_rows = 0

    try:
        for filename in os.listdir('/tmp'):
            if filename.endswith('.parquet') and not filename.startswith('monthly_taxi_stats') and not filename.startswith('daily_taxi_stats'):
                file_path = os.path.join('/tmp', filename)
                logger.info(f"Processing file: {filename}")

                for chunk in process_file(file_path):
                    df = transform_chunk(chunk)
                    total_rows += len(df)

                    # Update daily statistics
                    daily_chunk = df.groupby(pd.to_datetime(df['tpep_pickup_datetime']).dt.date).agg({
                        'tpep_pickup_datetime': 'count',
                        'trip_distance': ['mean', 'max', 'std', 'min', lambda x: np.percentile(x, 25), 'median', lambda x: np.percentile(x, 75)],
                        'total_amount': ['mean', 'sum']
                    }).reset_index()
                    daily_chunk.columns = ['trip_date', 'trip_count', 'avg_distance', 'max_distance', 'std_distance', 'min_distance', 'q1_distance', 'median_distance', 'q3_distance', 'avg_amount', 'total_amount']
                    daily_stats = pd.concat([daily_stats, daily_chunk]).groupby('trip_date').sum().reset_index()

                    # Update monthly statistics
                    monthly_chunk = df.groupby(pd.to_datetime(df['tpep_pickup_datetime']).dt.to_period('M')).agg({
                        'tpep_pickup_datetime': 'count',
                        'trip_distance': ['mean', 'max', 'std', 'min', lambda x: np.percentile(x, 25), 'median', lambda x: np.percentile(x, 75)],
                        'total_amount': ['mean', 'sum']
                    }).reset_index()
                    monthly_chunk.columns = ['trip_month', 'trip_count', 'avg_distance', 'max_distance', 'std_distance', 'min_distance', 'q1_distance', 'median_distance', 'q3_distance', 'avg_amount', 'total_amount']
                    monthly_stats = pd.concat([monthly_stats, monthly_chunk]).groupby('trip_month').sum().reset_index()

                logger.info(f"Processed {total_rows} rows so far...")

        # Final calculations for averages and percentiles
        for stats in [daily_stats, monthly_stats]:
            stats['avg_distance'] = stats['avg_distance'] * stats['trip_count'] / stats['trip_count'].sum()
            stats['avg_amount'] = stats['avg_amount'] * stats['trip_count'] / stats['trip_count'].sum()
            for col in ['q1_distance', 'median_distance', 'q3_distance']:
                stats[col] = stats[col] * stats['trip_count'] / stats['trip_count'].sum()

        # Convert trip_month to timestamp for monthly_stats
        monthly_stats['trip_month'] = monthly_stats['trip_month'].dt.to_timestamp()

        # Save the transformed data
        logger.info("Saving transformed data...")
        pq.write_table(pa.Table.from_pandas(daily_stats), '/tmp/daily_taxi_stats.parquet', compression='snappy')
        pq.write_table(pa.Table.from_pandas(monthly_stats), '/tmp/monthly_taxi_stats.parquet', compression='snappy')

        logger.info("Data transformation completed successfully")
        end_time = datetime.now()
        logger.info(f"Total processing time: {end_time - start_time}")

    except Exception as e:
        logger.error(f"An error occurred during transformation: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    run()