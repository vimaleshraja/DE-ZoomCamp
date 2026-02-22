# 🚕 NYC Taxi Data Engineering Project (BigQuery + dbt)

This project builds an end-to-end data pipeline using:

* Python (data ingestion)
* Google Cloud Storage (raw storage)
* BigQuery (data warehouse)
* dbt (transformations: staging → intermediate → marts)

Follow the steps below to reproduce the full data model.

---

# 📌 Prerequisites

* GCP Project with BigQuery enabled
* Service Account with:

  * BigQuery Data Editor
  * BigQuery Job User
  * Storage Admin
* Google Cloud Storage bucket
* Python 3.9+
* dbt-bigquery installed

---

# 🟢 Step 1 — Upload Taxi Data to GCS

## 1️⃣ Load Yellow & Green Taxi Data

File: `load_yellow_green.py`

This script:

* Downloads 2019–2020 Yellow and Green taxi data
* Converts data to Parquet
* Uploads files to your GCS bucket

### Update inside the file:

```python
BUCKET_NAME = "your-bucket-name"
PROJECT_ID = "your-project-id"
```

### Run:

```bash
python load_yellow_green.py
```

---

## 2️⃣ Load FHV Taxi Data

File: `load_fhv_tripdata.py`

This script:

* Uploads FHV trip `csv.gz` files
* Stores them in your GCS bucket

### Update inside the file:

```python
BUCKET_NAME = "your-bucket-name"
PROJECT_ID = "your-project-id"
```

### Run:

```bash
python load_fhv_tripdata.py
```

After this step, all raw data should be available in your Cloud Storage bucket.

---

# 🏗 Step 2 — Create BigQuery Tables

Open BigQuery Console and run:

```
create_green_yellow_dataset.sql
```

This script:

* Creates required datasets (if not existing)
* Creates tables from:

  * Parquet files (Yellow & Green)
  * CSV.GZ files (FHV)

After execution, verify:

* Yellow trip table
* Green trip table
* FHV trip table

---

# 🔄 Step 3 — Run dbt Transformations

Navigate into the dbt folder:

```bash
cd dbt
```

Install dependencies:

```bash
dbt deps
```

Build full project:

```bash
dbt build --target prod
```

This will create:

* `dbt_prod_staging`
* `dbt_prod_intermediate`
* `dbt_prod_marts`

---

# 🌱 Load Seed Data (If Needed)

```bash
dbt seed --target prod
```

---

# 🧪 Run Tests

```bash
dbt test --target prod
```

---

# 📊 Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

---

# 🗂 Data Model Layers

### Staging

* Cleaned raw source tables

### Intermediate

* Business logic transformations
* Derived fields and joins

### Marts

* Final fact and dimension tables
* Analytics-ready datasets

---

# ✅ Final Output

After completing all steps:

1. Data uploaded to GCS
2. BigQuery tables created
3. dbt models built

---

