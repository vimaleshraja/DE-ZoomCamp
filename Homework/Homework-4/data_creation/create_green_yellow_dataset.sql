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

--Question 3. Q3: Count of records in fct_monthly_zone_revenue? 
SELECT COUNT(*) FROM `de-zoomcamp-485500.dbt_raja.fct_monthly_zone_revenue` LIMIT 1000;

--Question 4. Q4: Zone with highest revenue for Green taxis in 2020?
SELECT pickup_zone, MAX(revenue_monthly_total_amount) AS revenue
FROM `de-zoomcamp-485500.dbt_raja.fct_monthly_zone_revenue`
WHERE service_type = 'Green' AND revenue_month >='2020-01-01'
GROUP BY pickup_zone
ORDER BY revenue DESC;

--Question 5. Q5: Total trips for Green taxis in October 2019? 
SELECT SUM(total_monthly_trips)
FROM `de-zoomcamp-485500.dbt_raja.fct_monthly_zone_revenue`
WHERE service_type = 'Green' AND revenue_month = '2019-10-01';

--Question 6. Q6: Count of records in stg_fhv_tripdata

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-485500.zoomcamp.fhv_2019_hw4`
OPTIONS (
  format = 'CSV',
  uris = ['gs://de-zoomcamp-hw4-2026-fhv/fhv_tripdata_2019-*.csv.gz'],
  compression = 'GZIP',
  skip_leading_rows = 1
);

SELECT COUNT(*) FROM `de-zoomcamp-485500.dbt_raja.stg_fhv_tripdata` LIMIT 1000

