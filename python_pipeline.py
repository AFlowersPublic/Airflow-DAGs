import pandas as pd

from datetime import datetime, timedelta

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'Alex'
}

# Python Function

def extract_data():
    df = pd.read_csv('https://raw.githubusercontent.com/AFlowersPublic/Airflow-DAGs/refs/heads/main/datasets/insurance.csv')
    print(df)
    return df.to_json()

def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='extract_data')
    
    df = pd.read_json(json_data)
    df = df.dropna()

    print(df)

    return df.to_json()


# DAG Implementation

with DAG(
    dag_id = 'python_pipeline',
    description = 'Insurance dataset extract / transform',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = None,
    tags = ['python', 'extract', 'transform', 'pipeline']
) as dag:
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )

extract_data >> remove_null_values