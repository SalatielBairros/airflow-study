from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

yesterday = pendulum.today('UTC').add(days=-1)

with DAG(
    dag_id='atividade_aula_4',
    start_date=yesterday,
    schedule='0 0 * * *'
) as dag:
    
    def cumprimentos():
        print("Olá! Este é o meu DAG de atividade da aula 4.")

    tarefa = PythonOperator(
        task_id = 'tarefa_1',
        python_callable=cumprimentos
    )
    
tarefa