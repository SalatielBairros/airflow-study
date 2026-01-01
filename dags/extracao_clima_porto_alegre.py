import sys
from pathlib import Path

# Adicionar o diretório raiz do Airflow ao sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from airflow import DAG
from operators.weather_operator import WeatherOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

base_filepath = '/Users/salatiel/airflow/data/weather/extraction'

with DAG(
    dag_id='weather_extraction_porto_alegre',
    dag_display_name='Extração Clima Porto Alegre',
    start_date=pendulum.datetime(2025, 12, 1, tz="America/Sao_Paulo"),
    schedule='0 4 * * *',
    catchup=False
) as dag:

    criar_diretorio = BashOperator(
        task_id = 'criar_diretorio',
        bash_command='mkdir -p ' + base_filepath)
    
    today = datetime.now()
    next_week = today + timedelta(days=7)
    
    weather_extraction = WeatherOperator(
        task_id = 'run_extraction',
        city='Porto Alegre', 
        start_time=today.strftime("%Y-%m-%d"),
        end_time=next_week.strftime("%Y-%m-%d"),        
        path=base_filepath
    )

    bronze_transform = SparkSubmitOperator(
        task_id='bronze_transform',
        application='/Users/salatiel/airflow/transformations/bronze_weather_transformations.py',
        name='bronze_weather_transformation',
        conn_id='spark_local',
        application_args=[
            '--src', '/Users/salatiel/airflow/data/weather/extraction/' + today.strftime("%Y%m%d") + '.json',
            '--dest', '/Users/salatiel/airflow/data/weather/bronze'
            ]
    )

    silver_transform = SparkSubmitOperator(
        task_id='silver_transform',
        application='/Users/salatiel/airflow/transformations/silver_weather_transformations.py',
        name='silver_weather_transformation',
        conn_id='spark_local',
        application_args=[
            '--src', '/Users/salatiel/airflow/data/weather/bronze',
            '--dest', '/Users/salatiel/airflow/data/weather/silver',
            '--date', today.strftime("%Y%m%d")
            ]
    )

    gold_transform = SparkSubmitOperator(
        task_id='gold_transform',
        application='/Users/salatiel/airflow/transformations/gold_weather_transformations.py',
        name='gold_weather_transformation',
        conn_id='spark_local',
        application_args=[
            '--src', '/Users/salatiel/airflow/data/weather/silver',
            '--dest', '/Users/salatiel/airflow/data/weather/gold'
            ]
    )
    
    
criar_diretorio >> weather_extraction >> bronze_transform >> silver_transform >> gold_transform