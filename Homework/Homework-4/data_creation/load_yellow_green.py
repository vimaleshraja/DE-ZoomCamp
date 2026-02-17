import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden


# ---------------- CONFIG ---------------- #

PROJECT_ID = "de-zoomcamp-485500"

GREEN_BUCKET = "de-zoomcamp-hw4-2026-green"
YELLOW_BUCKET = "de-zoomcamp-hw4-2026-yellow"

GREEN_DIR = "data/Homework-4/green"
YELLOW_DIR = "data/Homework-4/yellow"

CHUNK_SIZE = 8 * 1024 * 1024

client = storage.Client(project=PROJECT_ID)


# ---------------- BUCKET HELPERS ---------------- #

def create_bucket(bucket_name):
    try:
        bucket = client.get_bucket(bucket_name)

        project_bucket_ids = [bckt.name for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(f"Bucket '{bucket_name}' exists. Proceeding...")
        else:
            print(f"Bucket '{bucket_name}' exists but not in your project.")
            sys.exit(1)

    except NotFound:
        client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")

    except Forbidden:
        print(f"Bucket '{bucket_name}' exists but you do not have access.")
        sys.exit(1)


def verify_upload(bucket, blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


# ---------------- UPLOAD FUNCTION ---------------- #

def upload_file(file_path, bucket_name, max_retries=3):
    bucket = client.bucket(bucket_name)
    blob_name = os.path.basename(file_path)

    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} → {bucket_name} (Attempt {attempt+1})")

            blob.upload_from_filename(file_path)

            if verify_upload(bucket, blob_name):
                print(f"Upload verified: gs://{bucket_name}/{blob_name}")
                return
            else:
                print("Verification failed. Retrying...")

        except Exception as e:
            print(f"Upload error: {e}")

        time.sleep(5)

    print(f"Failed after {max_retries} attempts → {file_path}")


# ---------------- FILE COLLECTORS ---------------- #

def get_parquet_files(directory):
    return [
        os.path.join(directory, file)
        for file in os.listdir(directory)
        if file.endswith(".parquet")
    ]


# ---------------- MAIN ---------------- #

if __name__ == "__main__":

    # Create both buckets
    create_bucket(GREEN_BUCKET)
    create_bucket(YELLOW_BUCKET)

    # Collect files
    green_files = get_parquet_files(GREEN_DIR)
    yellow_files = get_parquet_files(YELLOW_DIR)

    # Upload GREEN files
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda f: upload_file(f, GREEN_BUCKET), green_files)

    # Upload YELLOW files
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda f: upload_file(f, YELLOW_BUCKET), yellow_files)

    print("✅ All parquet files uploaded successfully.")