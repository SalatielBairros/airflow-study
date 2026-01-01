from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse

def two_days_forecast(spark: SparkSession, silver_path: str, gold_path: str):
    weather_df = spark.read.json(f"{silver_path}/daily_weather")
    w = Window.partitionBy("date").orderBy(F.col("processed_date").desc())

    latest_per_date_df = (
        weather_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    
    two_days_df = weather_df.filter(
        (weather_df['date'] >= F.current_date()) & 
        (weather_df['date'] < F.date_add(F.current_date(), 3))
    )
    
    two_days_df.write.mode('overwrite').json(f"{gold_path}/two_days_forecast")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Gold Transformations")
    parser.add_argument("--src", type=str, required=True, help="Path to the input JSON file")
    parser.add_argument("--dest", type=str, required=True, help="Path to the input JSON file")
    args = parser.parse_args()

    spark = (SparkSession
        .builder
        .appName("Weather Gold Transformations")
        .getOrCreate())
    
    two_days_forecast(
        spark, 
        silver_path=args.src,
        gold_path=args.dest)