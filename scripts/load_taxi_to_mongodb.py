"""
load_taxi_to_mongodb.py

Load NYC Yellow Taxi 2022 trip data (Parquet shards) from GCS into MongoDB.
Handles Decimal conversion, datetime conversion, and batch insertion safely.
"""

from google.cloud import storage
import pandas as pd
from pymongo import MongoClient
import tempfile
import decimal

# ----------------------
# Configuration
# ----------------------
BUCKET_NAME = "dds-nyc-taxi-weather"
TAXI_PREFIX = "raw/taxi/yellow_"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "nyc_taxi_weather"
COLLECTION_NAME = "taxi_trips"
CHUNK_SIZE = 100_000  # number of rows per batch

# ----------------------
# Initialize GCS client
# ----------------------
storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
blobs = bucket.list_blobs(prefix=TAXI_PREFIX)
parquet_files = [blob.name for blob in blobs if blob.name.endswith(".parquet")]

# ----------------------
# Connect to MongoDB
# ----------------------
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
collection = db[COLLECTION_NAME]
collection.drop()  # start fresh

print(f"Found {len(parquet_files)} Parquet shards to load.")

# ----------------------
# Load each Parquet shard into MongoDB
# ----------------------
for file in parquet_files:
    print(f"Loading {file}...")
    blob = bucket.blob(file)
    with tempfile.NamedTemporaryFile() as tmp:
        blob.download_to_filename(tmp.name)
        df = pd.read_parquet(tmp.name)

    # --- Convert any Decimal objects to float safely ---
    for col in df.columns:
        if df[col].dtype == 'O':  # object type might hold decimals
            if df[col].apply(lambda x: isinstance(x, decimal.Decimal)).any():
                df[col] = df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)

    # --- Convert datetime columns ---
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].dt.to_pydatetime()

    # --- Insert in batches ---
    for start in range(0, len(df), CHUNK_SIZE):
        end = start + CHUNK_SIZE
        batch = df.iloc[start:end].to_dict("records")
        collection.insert_many(batch)

    print(f"Finished loading {file}, total rows so far: {collection.count_documents({})}")

print("All taxi shards loaded successfully!")
print(f"Total records in collection: {collection.count_documents({})}")