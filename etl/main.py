import download_data
import transform_data
import load_data
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_etl():
    logger.info("Starting ETL process...")
    try:
        download_data.run()
        logger.info("Data downloaded successfully.")
        
        transform_data.run()
        logger.info("Data transformed successfully.")
        
        load_data.run()
        logger.info("Data loaded successfully.")
        
        logger.info("ETL process completed.")
    except Exception as e:
        logger.error(f"An error occurred during ETL process: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    run_etl()