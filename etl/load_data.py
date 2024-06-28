import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run():
    db_url = os.getenv('DATABASE_URL')
    engine = create_engine(db_url)

    try:
        # Load daily stats
        logger.info("Loading data into refined.daily_taxi_stats table.")
        daily_stats_table = pq.read_table('/tmp/daily_taxi_stats.parquet')
        daily_stats_df = daily_stats_table.to_pandas()
        daily_stats_df.to_sql('daily_taxi_stats', engine, schema='refined', if_exists='replace', index=False)

        # Load monthly stats
        logger.info("Loading data into refined.monthly_taxi_stats table.")
        monthly_stats_table = pq.read_table('/tmp/monthly_taxi_stats.parquet')
        monthly_stats_df = monthly_stats_table.to_pandas()
        monthly_stats_df.to_sql('monthly_taxi_stats', engine, schema='refined', if_exists='replace', index=False)

        # Create a sample of raw data for the raw.taxi_data table
        logger.info("Creating sample data for raw.taxi_data table.")
        with engine.connect() as connection:
            connection.execute(text("""
            INSERT INTO raw.taxi_data (
                tpep_pickup_datetime, 
                tpep_dropoff_datetime, 
                passenger_count, 
                trip_distance, 
                total_amount
            )
            SELECT 
                trip_date as tpep_pickup_datetime,
                trip_date + INTERVAL '30 minutes' as tpep_dropoff_datetime,
                FLOOR(RANDOM() * 4 + 1)::INT as passenger_count,
                avg_distance as trip_distance,
                avg_amount as total_amount
            FROM refined.daily_taxi_stats
            LIMIT 1000  -- Adjust this number as needed
            """))

        # Create a sample of trusted data
        logger.info("Creating sample data for trusted.taxi_data table.")
        with engine.connect() as connection:
            connection.execute(text("""
            INSERT INTO trusted.taxi_data
            SELECT * FROM raw.taxi_data
            WHERE trip_distance > 0 AND total_amount > 0
            """))

        logger.info("Data loading completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during data loading: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    run()