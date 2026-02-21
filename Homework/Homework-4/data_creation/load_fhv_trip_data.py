import os
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden

# ---------------- CONFIG ---------------- #

PROJECT_ID = "de-zoomcamp-485500"
FHV_BUCKET = "de-zoomcamp-hw4-2026-fhv"
LOCAL_DIR = "data/Homework-4/fhv"

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"

YEAR = 2019
MONTHS = [f"{i:02d}" for i in range(1, 13)]  # 01 → 12

CHUNK_SIZE = 8 * 1024 * 1024

client = storage.Client(project=PROJECT_ID)

os.makedirs(LOCAL_DIR, exist_ok=True)

# ---------------- BUCKET HELPERS ---------------- #

def create_bucket(bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' exists. Proceeding...")

    except NotFound:
        client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")

    except Forbidden:
        print(f"No access to bucket '{bucket_name}'")
        sys.exit(1)


def verify_upload(bucket, blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


# ---------------- DOWNLOAD FUNCTION ---------------- #

def download_file(year, month):
    filename = f"fhv_tripdata_{year}-{month}.csv.gz"
    url = f"{BASE_URL}/{filename}"
    local_path = os.path.join(LOCAL_DIR, filename)

    if os.path.exists(local_path):
        print(f"{filename} already exists locally.")
        return local_path

    print(f"Downloading {filename}...")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)

    print(f"Downloaded {filename}")
    return local_path


# ---------------- UPLOAD FUNCTION ---------------- #

def upload_file(file_path, bucket_name, max_retries=3):
    bucket = client.bucket(bucket_name)
    blob_name = os.path.basename(file_path)

    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {blob_name} (Attempt {attempt+1})")

            blob.upload_from_filename(file_path)

            if verify_upload(bucket, blob_name):
                print(f"Upload verified: gs://{bucket_name}/{blob_name}")
                return
            else:
                print("Verification failed. Retrying...")

        except Exception as e:
            print(f"Upload error: {e}")

        time.sleep(5)

    print(f"Failed after {max_retries} attempts → {blob_name}")


# ---------------- MAIN ---------------- #

if __name__ == "__main__":

    create_bucket(FHV_BUCKET)

    # Step 1: Download all files
    downloaded_files = []

    for month in MONTHS:
        file_path = download_file(YEAR, month)
        downloaded_files.append(file_path)

    # Step 2: Upload in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda f: upload_file(f, FHV_BUCKET), downloaded_files)

    print("All FHV 2019 csv.gz files uploaded successfully.")

