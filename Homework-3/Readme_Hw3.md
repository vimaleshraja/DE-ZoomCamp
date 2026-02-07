# NYC Yellow Taxi Data Engineering Project (DE Zoomcamp HW3)

This project demonstrates an End-to-End data pipeline starting from data ingestion in a **GitHub Codespace** to advanced querying and optimization in **Google BigQuery**.

## 🚀 Workflow Overview

### 1. Environment Setup
*   **Environment:** Used **GitHub Codespaces** as the primary IDE.
*   **Authentication:** Configured the [Google Cloud SDK (gcloud CLI)](https://cloud.google.com/sdk/docs/install) to authenticate the environment with Google Cloud Platform.
    ```bash
    gcloud auth application-default login
    ```

### 2. Data Ingestion & GCS Upload
*   **Programmatic Ingestion:** Developed a [Python script](https://github.com/vimaleshraja/DE-ZoomCamp/blob/main/Homework-3/load_yellow_taxi.py) to perform **automated data retrieval** from the NYC Taxi dataset URIs.
*   **Local Persistence:** The script downloaded the Parquet files and stored them in the Codespace's local filesystem.
*   **GCS Provisioning:** Created a [Google Cloud Storage (GCS)](https://cloud.google.com) bucket using the Google SDK.
*   **Staging:** Uploaded the local Parquet files to the GCS bucket to serve as the **Data Lake**.

### 3. BigQuery Implementation
*   **External Table:** Created an **External Table** in BigQuery to access the Parquet files directly from GCS without internal storage costs.
*   **Materialized Table:** Created a regular table (native storage) from the GCS source for high-performance querying.
*   **Optimization:** Implemented **Partitioning** (by Date) and **Clustering** (by VendorID) to optimize query time and reduce bytes processed for common filters.

---

## 💻 SQL Implementation & Homework Solutions

### Q1. Counting total records
```sql
SELECT count(*) FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`;
-- Answer: 20,332,093
```

---
### Q2. Data read estimation
```sql
-- External Read
SELECT COUNT(DISTINCT PULocationID) FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data`;

-- Materialized Read
SELECT COUNT(DISTINCT PULocationID) FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`;

-- Answer: 
-- External Table: 0 MB (Metadata only)
-- Materialized Table: 155.12 MB
```
---
### Q3. Understanding columnar storage
```sql
SELECT PULocationID
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`;

SELECT PULocationID, DOLocationID
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`;
--BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
```
---
### Q4. Counting zero fare trips
```sql
SELECT COUNT(fare_amount)
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`
WHERE fare_amount = 0;
-- Answer: 8,333
```
---
### Q5. Partitioning and clustering
```sql
CREATE OR REPLACE TABLE `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_partitioned` 
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`;

-- Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID
```
---
### Q6. Partition benefits
```sql
SELECT DISTINCT VendorID
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_non_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT VendorID
FROM `de-zoomcamp-485500.zoomcamp.yellow_taxi_data_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

```
---
### Q7. Counting total records
##### Answer: GCP Bucket
---
### Q8. Counting total records
##### Answer: False
---

## 🛠️ Tech Stack

| Component | Technology |
| :--- | :--- |
| **Cloud Platform** | [Google Cloud Platform (GCP)](https://cloud.google.com) |
| **Data Lake / Storage** | [Google Cloud Storage (GCS)](https://cloud.google.comstorage) |
| **Data Warehouse** | [BigQuery](https://cloud.google.combigquery) |
| **Development Environment** | [GitHub Codespaces](https://github.com) |
| **Programming Language** | [Python](https://www.python.org) |
| **CLI Tooling** | [Google Cloud SDK (gcloud)](https://cloud.google.comsdk) |

---

### 🔧 Key Implementation Details
*   **Ingestion:** Automated data retrieval via Python from Parquet URIs.
*   **Infrastructure:** Programmatic bucket creation and file staging using the `gcloud` CLI.
*   **Optimization:** Logic implemented via SQL for partitioning and clustering in BigQuery.