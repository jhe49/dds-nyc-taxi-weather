import os
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def run_ml_predictions():
    # ---- 1. SETUP ----
    ATLAS_URI = "mongodb+srv://jhe49_db_user:fDnLeld4Uug1cZhq@cluster0.haae1bq.mongodb.net/"
    DB_NAME = "nyc_taxi_weather"
    
    # Initialize Spark without the MongoDB connector jar to avoid conflicts
    spark = SparkSession.builder \
        .appName("NYC_Taxi_ML_Final") \
        .getOrCreate()

    print("Loading data via PyMongo/Pandas bridge...")
    client = MongoClient(ATLAS_URI)
    db = client[DB_NAME]
    
    # Load Taxi and Weather into Pandas first (The "Safety Bridge")
    taxi_data = list(db["taxi_trips_sample"].find({}, {"pickup_datetime": 1, "total_amount": 1, "_id": 0}))
    weather_data = list(db["weather_2022"].find({}, {"DATE": 1, "TMAX": 1, "PRCP": 1, "_id": 0}))
    
    pdf_taxi = pd.DataFrame(taxi_data)
    pdf_weather = pd.DataFrame(weather_data)
    
    # Convert to Spark DataFrames
    taxi_df = spark.createDataFrame(pdf_taxi)
    weather_df = spark.createDataFrame(pdf_weather)

    # ---- 2. DATA PREP ----
    print("Joining datasets in Spark...")
    # Convert dates for joining
    taxi_df = taxi_df.withColumn("pickup_date", col("pickup_datetime").cast("date"))
    weather_df = weather_df.withColumn("weather_date", col("DATE").cast("date"))
    
    joined_df = taxi_df.join(weather_df, taxi_df.pickup_date == weather_df.weather_date)

    # ---- 3. ML FEATURES ----
    print("Preparing ML features...")
    ml_df = joined_df.withColumn("hour", hour(col("pickup_datetime"))) \
                     .withColumn("dow", dayofweek(col("pickup_datetime"))) \
                     .select("total_amount", "TMAX", "PRCP", "hour", "dow") \
                     .dropna()

    assembler = VectorAssembler(inputCols=["TMAX", "PRCP", "hour", "dow"], outputCol="features")
    final_ml_data = assembler.transform(ml_df)
    train, test = final_ml_data.randomSplit([0.8, 0.2], seed=42)

    # ---- 4. TRAINING ----
    print("Training Random Forest Model...")
    rf = RandomForestRegressor(featuresCol="features", labelCol="total_amount")
    model = rf.fit(train)
    predictions = model.transform(test)

    evaluator = RegressionEvaluator(labelCol="total_amount", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"\n✅ SUCCESS! Final Model RMSE: {rmse:.2f}")

    # ---- 5. EXPORT ----
    print("Saving results to Atlas...")
    results = predictions.select("total_amount", "prediction").limit(10).toPandas().to_dict('records')
    db["ml_prediction_results"].delete_many({})
    db["ml_prediction_results"].insert_many(results)
    
    print("✅ Pipeline Complete. Check Compass for 'ml_prediction_results'!")
    spark.stop()

if __name__ == "__main__":
    run_ml_predictions()