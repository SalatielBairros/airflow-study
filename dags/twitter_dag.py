import sys
import os
from pathlib import Path

# Adicionar o diretório raiz do Airflow ao sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from airflow.models import DAG
from airflow.providers.standard.operators.bash import BashOperator
from operators.twitter_operator import TwitterOperator
import pendulum
from datetime import datetime, timedelta

yesterday = pendulum.today('UTC').add(days=-1)
base_filepath = '/Users/salatiel/airflow/data/twitter/dia='

with DAG(
    dag_id='TwitterDummyExtractor',
    start_date=pendulum.now(),
    schedule='0 0 * * *'
) as dag:    

    final_directory = base_filepath + '{{data_interval_end.strftime("%Y-%m-%d")}}'
    criar_diretorio = BashOperator(
        task_id = 'criar_diretorio',
        bash_command='mkdir -p ' + final_directory)

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "datascience"

    twitter_extraction = TwitterOperator(
        task_id = 'run_extraction',
        query=query, 
        start_time=start_time,
        end_time=end_time,
        path=final_directory
    )
    
criar_diretorio >> twitter_extraction