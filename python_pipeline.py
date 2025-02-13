import pandas as pd
from pathlib import Path

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'Alex'
}

# Python Functions

data_directory = Path(__file__).parent / "data_export"
data_directory.mkdir(exist_ok=True)

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

def groupby_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv(data_directory/'grouped_by_smoker.csv', index=False)

def groupby_region(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    regions_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    regions_df.to_csv(data_directory/'grouped_by_region.csv', index=False)

# DAG Implementation

with DAG(
    dag_id = 'python_pipeline_insurance_ET',
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

    groupby_smoker = PythonOperator(
        task_id='groupby_smoker',
        python_callable=groupby_smoker
    )

    groupby_region = PythonOperator(
        task_id='groupby_region',
        python_callable=groupby_region
    )

extract_data >> remove_null_values >> [groupby_smoker, groupby_region]