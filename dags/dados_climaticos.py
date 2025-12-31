from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.execution_time.macros import ds_add
import pendulum
import logging
from os.path import exists
import requests
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

start_date = pendulum.datetime(2025, 12, 1, tz="UTC")
weekly_interval = "0 0 * * 1"
base_filepath = '/Users/salatiel/airflow/data/tempo/semana='

with DAG(
    dag_id='dados_climaticos',
    start_date=start_date,
    schedule=weekly_interval,
    catchup=True
) as dag:
    
    criar_diretorio = BashOperator(
        task_id = 'criar_diretorio',
        bash_command='mkdir -p ' + base_filepath + '{{data_interval_end.strftime("%Y-%m-%d")}}')
    
    def extrai_dados(data_interval_end):
        data_inicio = data_interval_end
        data_fim = ds_add(data_interval_end, 6)

        city = 'Boston'
        key = 'JGWEBTDGKJ76Z82SRYXQ6X4J9'
        destiny_filepath = f'{base_filepath}{data_inicio}/'
        destiny_filename = f'{destiny_filepath}dados_brutos.csv'

        URL = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv'

        if not exists(destiny_filename):
            response = requests.get(URL, verify=False)

            if response.status_code == 200:
                with open(destiny_filename, 'wb') as file:
                    file.write(response.content)
                logging.info(f'Data successfully saved to {destiny_filename}')
            else:
                raise Exception(f'Error fetching data: {response.status_code} - {response.text}')
            
        else:
            logging.info(f'File {destiny_filename} already exists. Skipping download.')

        dados = pd.read_csv(destiny_filename)
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(destiny_filepath + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(destiny_filepath + 'condicoes.csv')

    extrair_dados = PythonOperator(
        task_id = 'extrair_dados',
        python_callable=extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

criar_diretorio >> extrair_dados