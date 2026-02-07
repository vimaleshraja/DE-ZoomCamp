-- External Table
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-485500.zoomcamp.yellow_taxi_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://de-zoomcamp-hw3-2026/yellow_tripdata_2024-*.parquet']
);

-- Materialized Table
CREATE OR REPLACE TABLE `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned` AS
SELECT * FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data`;

-- Q1. Counting records
SELECT count(*) FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`;

--Q2. Data read estimation
--0 MB for the External Table and 155.12 MB for the Materialized Table

--External Read
SELECT COUNT(DISTINCT a.PULocationID)
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data` AS a;

-- Materialized Read
SELECT COUNT(DISTINCT b.PULocationID)
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned` AS b;

--Q3. Understanding columnar storage
--BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

SELECT PULocationID
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`;

SELECT PULocationID, DOLocationID
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`;

--Q4. Counting zero fare trips
--8,333
SELECT COUNT(fare_amount)
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`
WHERE fare_amount = 0;

--Q5. Partitioning and clustering
--Partition by tpep_dropoff_datetime and Cluster on VendorID

CREATE OR REPLACE TABLE `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_partitioned` 
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`;

--Q6. Partition benefits
-- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

SELECT DISTINCT VendorID
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

--Q7. External table storage
--GCP Bucket

--Q8. Clustering best practices
-- False

--Q9. Understanding table scans
--0B
SELECT COUNT(*)
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`


