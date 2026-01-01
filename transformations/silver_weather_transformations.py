from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import hashlib
import argparse

def __generate_hash__(df: DataFrame, columns: list[str], hash_column_name: str = 'hashid') -> DataFrame:
    
    def hash_row(*args) -> str:
        hash_input = ''.join([str(arg) for arg in args if arg is not None])
        return hashlib.md5(hash_input.encode('utf-8')).hexdigest()

    hash_udf = F.udf(hash_row, returnType=F.StringType())

    return df.withColumn(
        hash_column_name,
        hash_udf(*[F.col(col) for col in columns])
    )

def add_hashid_to_stations(df: DataFrame) -> DataFrame:
    new_df = __generate_hash__(df, ['id', 'name'], 'record_hash')
    return __generate_hash__(new_df, ['id', 'latitude', 'longitude', 'name'], 'content_hash')

def add_hashid_to_daily_weather(df: DataFrame) -> DataFrame:
    new_df = __generate_hash__(df, ['date', 'weather_address', 'processed_date'], 'record_hash')
    return __generate_hash__(new_df, [ 'date', 'weather_address', 'processed_date',
            'cloudcover', 'conditions', 'description', 'feelslike', 'feelslikemax', 'feelslikemin', 'humidity', 'icon', 'precip',
            'precipcover', 'precipprob', 'preciptype', 'severerisk', 'solarenergy', 'solarradiation', 'temp', 'tempmax', 'tempmin', 'uvindex',
            'winddir', 'windspeed', 'windgust'
        ], 'content_hash')

def add_hashid_to_station_quality(df: DataFrame) -> DataFrame:
    new_df = __generate_hash__(df, ['station_id', 'processed_date', 'weather_address', 'distance'], 'record_hash')
    return __generate_hash__(new_df, ['station_id', 'processed_date', 'weather_address', 'distance', 'quality'], 'content_hash')

def transform(spark: SparkSession, silver_path: str, bronze_path: str, processed_date: str):
    stations_df = spark.read.json(f"{bronze_path}/stations/process_date={processed_date}")
    daily_weather_df = spark.read.json(f"{bronze_path}/daily_weather/process_date={processed_date}")
    station_quality_df = spark.read.json(f"{bronze_path}/station_quality/process_date={processed_date}")

    stations_with_hash_df = add_hashid_to_stations(stations_df)
    daily_weather_with_hash_df = add_hashid_to_daily_weather(daily_weather_df)
    station_quality_with_hash_df = add_hashid_to_station_quality(station_quality_df)

    stations_with_hash_df.write.mode('overwrite').json(f"{silver_path}/stations/processed_date={processed_date}")
    daily_weather_with_hash_df.write.mode('overwrite').json(f"{silver_path}/daily_weather/processed_date={processed_date}")
    station_quality_with_hash_df.write.mode('overwrite').json(f"{silver_path}/station_quality/processed_date={processed_date}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Silver Transformations")
    parser.add_argument("--src", type=str, required=True, help="Path to the input JSON file")
    parser.add_argument("--dest", type=str, required=True, help="Path to the input JSON file")
    parser.add_argument("--date", type=str, required=True, help="Processed date in YYYYMMDD format")
    args = parser.parse_args()

    spark = (SparkSession
        .builder
        .appName("Weather Silver Transformations")
        .getOrCreate())
    
    transform(
        spark, 
        bronze_path=args.src,
        silver_path=args.dest,
        processed_date=args.date)