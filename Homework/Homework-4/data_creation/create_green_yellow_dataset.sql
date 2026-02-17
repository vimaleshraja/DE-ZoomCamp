-- External Table green data
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-485500.zoomcamp.green_trip_hw4`
OPTIONS (
  format = 'parquet',
  uris = ['gs://de-zoomcamp-hw4-2026-green/green_tripdata_*.parquet']
);


-- Materialized Table green data
CREATE OR REPLACE TABLE `de-zoomcamp-485500.zoomcamp.green_trip` 
PARTITION BY DATE(`lpep_pickup_datetime`)
CLUSTER BY VendorID
AS
SELECT * EXCEPT(ehail_fee, congestion_surcharge) FROM `de-zoomcamp-485500.zoomcamp.green_trip_hw4`;


-- External Table Yellow data
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-485500.zoomcamp.yellow_trip_hw4`
OPTIONS (
  format = 'parquet',
  uris = ['gs://de-zoomcamp-hw4-2026-yellow/yellow_tripdata_*.parquet']
);


-- Materialized Table Yellow data
CREATE OR REPLACE TABLE `de-zoomcamp-485500.zoomcamp.yellow_trip` 
PARTITION BY DATE(`tpep_pickup_datetime`)
CLUSTER BY VendorID
AS
SELECT 
      * EXCEPT(congestion_surcharge),
FROM `de-zoomcamp-485500.zoomcamp.yellow_trip_hw4`;
