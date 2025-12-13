from airflow.models import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

yesterday = pendulum.today('UTC').add(days=-2)

with DAG(
    dag_id='meu_primeiro_dag',
    start_date=yesterday,
    schedule='0 0 * * *'
) as dag:
    tarefa_1 = EmptyOperator(task_id = 'tarefa_1')
    tarefa_2 = EmptyOperator(task_id = 'tarefa_2')
    tarefa_3 = EmptyOperator(task_id = 'tarefa_3')
    tarefa_4 = BashOperator(
        task_id = 'cria_pasta',
        bash_command='mkdir -p /tmp/minha_pasta={{data_interval_end}}')
    
tarefa_1 >> [tarefa_2, tarefa_3] 
tarefa_3 >> tarefa_4