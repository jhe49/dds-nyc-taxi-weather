"""
load_weather_to_mongodb.py

Load NYC Central Park weather data (CSV) into MongoDB.
"""

import pandas as pd
from pymongo import MongoClient

# ----------------------
# Configuration
# ----------------------
FILE_PATH = "/Users/joshuahe/Downloads/NYC_Central_Park_weather_1869-2022.csv"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "nyc_taxi_weather"
COLLECTION_NAME = "weather"

# ----------------------
# Connect to MongoDB
# ----------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Optional: drop collection so you don’t duplicate data
collection.drop()

# ----------------------
# Load CSV
# ----------------------
df = pd.read_csv(FILE_PATH)

# Convert DATE column to datetime (important for aggregation later)
if "DATE" in df.columns:
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

# ----------------------
# Insert into MongoDB
# ----------------------
collection.insert_many(df.to_dict("records"))

print("Weather data loaded successfully!")
print(f"Total records in collection: {collection.count_documents({})}")