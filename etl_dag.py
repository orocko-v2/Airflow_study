import datetime
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

args = {
    'owner': 'orocko',
    'start_date': datetime.datetime(2025, 11, 28),
    'provide_context': True
}

api_link = 'https://api.spacexdata.com/v4/rockets'

def extract_data(**kwargs):
    ti = kwargs['ti']

    responce =  requests.get(api_link)
    print(responce.status_code)
    if responce.status_code == 200:
        rocket_data = responce.json()
        print(rocket_data)

        ti.xcom_push(key='spacex_rockets_json', value=rocket_data)

def transform_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='extract', key='spacex_rockets_json')
    columns = ['name', 
                'height', 
                'diameter',
                'mass',
                'is_active',
                'first_flight']
    df = pd.DataFrame(columns = columns)
    print('data', json_data)
    for rocket in json_data:
        print('rocket', rocket)
        name = rocket['name']
        height = rocket['height']['meters']
        diameter = rocket['diameter']['meters']
        mass = rocket['mass']['kg']
        is_active = rocket['active']
        first_flight = rocket['first_flight']

        value_list = [name, 
                      height, 
                      diameter,
                      mass,
                      is_active,
                      first_flight]
        print('list', value_list)
        new_df = pd.DataFrame([value_list], columns = columns)
        df = pd.concat([df, new_df], ignore_index=True)

    print(df.head())
    ti.xcom_push(key='spacex_rockets_df', value=df)

def load_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids = 'transform', key='spacex_rockets_df')
    print(df.head())

with DAG('load_spacex', schedule_interval='*/1 * * * *', catchup=False,default_args=args) as dag:
    extract_data = PythonOperator(task_id='extract', python_callable=extract_data)
    transform_data = PythonOperator(task_id='transform', python_callable=transform_data)
    load_data = PythonOperator(task_id='load', python_callable=load_data)

    extract_data >> transform_data >> load_data