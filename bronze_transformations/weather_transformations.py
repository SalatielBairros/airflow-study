from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import argparse

BASE_PATH = "data/weather/bronze"

def __get_used_stations__(df: DataFrame) -> DataFrame:
    station_names = df.select("stations.*").columns

    df_with_array = df.withColumn(
        "stations_array", 
        F.array(*[F.col(f"stations.{name}") for name in station_names])
    )

    return df_with_array.select(F.explode("stations_array").alias("station")).select("station.*")

def __get_processed_date__(df: DataFrame) -> str:
    return df.select(F.min(F.array_min("days.datetime"))).collect()[0][0]

def __get_location_data__(df: DataFrame):
    return (df
                .select(['latitude', 'longitude', 'resolvedAddress'])
                .withColumnsRenamed({
                    'latitude': 'weather_latitude',
                    'longitude': 'weather_longitude',
                    'resolvedAddress': 'weather_address'
                })
                .first())

def get_stations(df: DataFrame) -> DataFrame:    
    return __get_used_stations__(df).select(['id', 'latitude', 'longitude', 'name'])

def get_station_quality(df: DataFrame):
    processed_date = __get_processed_date__(df)
    df_used_stations = __get_used_stations__(df)
    location_data = __get_location_data__(df)

    return (df_used_stations
                        .select(['id', 'distance', 'quality'])
                        .withColumnRenamed('id', 'station_id')
                        .withColumns({
                                'processed_date': F.lit(processed_date),
                                'weather_latitude': F.lit(location_data['weather_latitude']),
                                'weather_longitude': F.lit(location_data['weather_longitude']),
                                'weather_address': F.lit(location_data['weather_address'])
                        }))

def get_daily_weather(df: DataFrame) -> DataFrame:
    processed_date = __get_processed_date__(df)
    location_data = __get_location_data__(df)

    return df.select(F.explode('days').alias('day')).select('day.*').select(
        [
            'cloudcover', 'conditions', 'datetime', 'description', 'feelslike', 'feelslikemax', 'feelslikemin', 'humidity', 'icon', 'precip',
            'precipcover', 'precipprob', 'preciptype', 'severerisk', 'solarenergy', 'solarradiation', 'temp', 'tempmax', 'tempmin', 'uvindex',
            'winddir', 'windspeed', 'windgust'
        ]).withColumnRenamed('datetime', 'date').withColumns({
                            'processed_date': F.lit(processed_date),
                            'weather_latitude': F.lit(location_data['weather_latitude']),
                            'weather_longitude': F.lit(location_data['weather_longitude']),
                            'weather_address': F.lit(location_data['weather_address'])
                      })

def export_json(df: DataFrame, output_path: str):    
    df.coalesce(1).write.mode("overwrite").json(output_path)

def transform(spark: SparkSession, input_path: str, output_path: str):
    df = spark.read.json(input_path)

    stations_df = get_stations(df)
    station_quality_df = get_station_quality(df)
    daily_weather_df = get_daily_weather(df)

    process_date = __get_processed_date__(df).replace("-", "")

    export_json(stations_df, f"{output_path}/stations/process_date={process_date}")
    export_json(station_quality_df, f"{output_path}/station_quality/process_date={process_date}")
    export_json(daily_weather_df, f"{output_path}/daily_weather/process_date={process_date}")

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Bronze Transformations")
    parser.add_argument("--input_file", type=str, required=True, help="File name to be processed from the extraction layer")
    args = parser.parse_args()

    spark = (SparkSession
        .builder
        .appName("Weather Bronze Transformations")
        .getOrCreate())
    
    transform(
        spark, 
        input_path=f"../data/weather/extraction/{args.input_file}",
        output_path=f"../{BASE_PATH}",)