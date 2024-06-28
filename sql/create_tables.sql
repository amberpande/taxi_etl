-- Raw Layer
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.taxi_data (
    id SERIAL PRIMARY KEY,
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count FLOAT,
    trip_distance FLOAT,
    RatecodeID FLOAT,
    store_and_fwd_flag VARCHAR(1),
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT
);

-- Trusted Layer
CREATE SCHEMA IF NOT EXISTS trusted;

CREATE TABLE IF NOT EXISTS trusted.taxi_data (
    id SERIAL PRIMARY KEY,
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    RatecodeID INTEGER,
    store_and_fwd_flag VARCHAR(1),
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT
);

-- Refined Layer
CREATE SCHEMA IF NOT EXISTS refined;

CREATE TABLE IF NOT EXISTS refined.daily_taxi_stats (
    trip_date DATE PRIMARY KEY,
    trip_count INTEGER,
    avg_distance FLOAT,
    avg_amount FLOAT,
    max_distance FLOAT,
    total_amount FLOAT,
    std_distance FLOAT,
    min_distance FLOAT,
    q1_distance FLOAT,
    median_distance FLOAT,
    q3_distance FLOAT
);

CREATE TABLE IF NOT EXISTS refined.monthly_taxi_stats (
    trip_month DATE PRIMARY KEY,
    trip_count INTEGER,
    avg_distance FLOAT,
    avg_amount FLOAT,
    max_distance FLOAT,
    total_amount FLOAT,
    std_distance FLOAT,
    min_distance FLOAT,
    q1_distance FLOAT,
    median_distance FLOAT,
    q3_distance FLOAT
);

CREATE INDEX idx_daily_stats_trip_date ON refined.daily_taxi_stats(trip_date);