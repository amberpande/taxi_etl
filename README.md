# NYC Taxi Data ETL and Analysis

This project performs ETL (Extract, Transform, Load) operations on New York City taxi trip data and provides an API for accessing aggregated statistics.

## Process Steps

1. **Extract**: Download Parquet files containing NYC taxi trip data for each month of 2023.
2. **Transform**: Process the raw data, handling any inconsistencies in column names and data types.
3. **Load**: Insert the processed data into a PostgreSQL database, organizing it into raw, trusted, and refined layers.
4. **API**: Provide an endpoint to access aggregated statistics from the processed data.

## Decisions Made

1. Used Python for ETL process due to its rich ecosystem of data processing libraries.
2. Chose PostgreSQL as the database for its robustness and support for complex queries.
3. Implemented a multi-layer data architecture (raw, trusted, refined) for data quality and performance optimization.
4. Used Docker and Docker Compose for easy setup and deployment of the entire system.
5. Implemented the API using Flask for its simplicity and ease of use.

## Answers to Questions

1. **Total number of records in the final table:**
   ```sql
   SELECT COUNT(*) FROM trusted.taxi_data; -- 38310226

2. **Total number of trips started and completed on June 17th:**
   ```sql
   SELECT COUNT(*) FROM trusted.taxi_data 
   WHERE DATE(tpep_pickup_datetime) = '2023-06-17' 
   AND DATE(tpep_dropoff_datetime) = '2023-06-17'; -- 106180

2. **Day of the longest trip traveled:**
   ```sql
   SELECT DATE(tpep_pickup_datetime) AS trip_date
    FROM trusted.taxi_data
    ORDER BY trip_distance DESC
    LIMIT 1; -- 2023-08-15

2. **Mean, standard deviation, minimum, maximum and quartiles of the distribution of distance traveled in total trips:**
   ```sql
   SELECT 
    AVG(trip_distance) AS mean,
    STDDEV(trip_distance) AS std_dev,
    MIN(trip_distance) AS min,
    MAX(trip_distance) AS max,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance) AS q1,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trip_distance) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) AS q3
    FROM trusted.taxi_data; 
    
    -- "max": 362967.13, 
    -- "mean": 0.08305980870604987, 
    -- "median": 0.023250796851155952, 
    -- "min": 0.0, 
    -- "q1": 0.01453594341400726, 
    -- "q3": 0.041642707892448914, 
    -- "std_dev": 784.004374551888

## API Endpoint

The aggregated statistics can be accessed via a single API endpoint:
**GET /aggregates**

The aggregated statistics can be accessed via a single API endpoint:
**GET /aggregates**

This endpoint returns a JSON object containing all the required statistics.

## Setup and Running

1. Ensure Docker and Docker Compose are installed on your system.
2. Clone this repository.
3. Create a .env file in the project root and set the POSTGRES_PASSWORD.
4. Run docker-compose up --build to start the system.
5. The API will be available at http://localhost:5001/aggregates after the ETL completes.

## Project Structure

- etl/: Contains Python scripts for the ETL process.
- api/: Contains the Flask app for the API.
- sql/: Contains SQL scripts for initializing the database.
- Dockerfile: Defines the Docker image for the project.
- docker-compose.yml: Defines the multi-container Docker application.
- requirements.txt: Lists Python dependencies.

## Future Improvements

- Implement Apache Airflow and develop the pipeline as a DAG.
- Implement data quality checks in the ETL process.
- Add more comprehensive error handling and logging.
- Implement incremental updates to handle new data efficiently.
- Add authentication to the API.

