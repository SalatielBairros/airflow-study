import sys
from pathlib import Path

# Adicionar o diretório raiz do Airflow ao sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from airflow import DAG
from operators.weather_operator import WeatherOperator
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
import logging
from os.path import exists
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

base_filepath = '/Users/salatiel/airflow/data/weather/extraction'

with DAG(
    dag_id='weather_extraction_porto_alegre',
    dag_display_name='Extração Clima Porto Alegre',
    start_date=pendulum.now(),
    schedule='0 0 * * *',
    catchup=True
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
    
    

criar_diretorio >> weather_extraction