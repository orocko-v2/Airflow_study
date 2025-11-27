from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

args = {
    'owner': 'orocko',
    'start_date': datetime(2025, 11, 27), 
    'provide_context': True
}

with DAG(dag_id='Hello-world', description='Hello world dag', schedule_interval='*/1 * * * *', catchup=False, default_args=args) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command = 'echo "Hello world from task 1"'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command = 'echo "Hello world from task 2"'
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command = 'echo "Hello world from task 3"'
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command = 'echo "Hello world from task 4"'
    )

    task1 >> task2
    task1 >> task3
    task2 >> task4
    task3 >> task4