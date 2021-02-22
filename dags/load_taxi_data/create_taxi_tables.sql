CREATE TABLE IF NOT EXISTS raw_data.yellow_taxi_trips
(
    vendor_id        TEXT,
    pickup_datetime  TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count  NUMERIC,
    PULocationID     TEXT,
    DOLocationID     TEXT
);

CREATE TABLE IF NOT EXISTS raw_data.green_taxi_trips
(
    VendorID         TEXT,
    pickup_datetime  TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count  NUMERIC,
    PULocationID     TEXT,
    DOLocationID     TEXT
);


DROP TABLE IF EXISTS raw_data.location_mapping;
CREATE TABLE raw_data.location_mapping
(
    location_id  INT PRIMARY KEY,
    borough      TEXT,
    zone         TEXT,
    service_zone TEXT
);
