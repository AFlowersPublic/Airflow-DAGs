import pandas as pd
import sqlite3
from pathlib import Path

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator

data_directory = Path(__file__).parent / 'data_export'
data_directory.mkdir(exist_ok=True)

def extract_data():
    df = pd.read_csv('https://raw.githubusercontent.com/AFlowersPublic/Airflow-DAGs/refs/heads/main/datasets/insurance.csv')
    print(df)
    return df.to_json()

def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='extract_data')
    
    df = pd.read_json(json_data)
    df = df.dropna(how='all')
    df.reset_index(inplace=True,drop=True)

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

def insert_values(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)
    
    conn = sqlite3.connect(Path(__file__).parent / 'database/ins_sqlite.db')

    df.to_sql('insurance_charges',conn,if_exists='append',index_label='id')

# DAG Implementation

default_args = {
    'owner' : 'Alex'
}

with DAG(
    dag_id = 'python_pipeline_insurance_ETL',
    description = 'Insurance dataset ETL utilizing Python / SQL',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = None,
    tags = ['python', 'SQL', 'extract', 'transform', 'load', 'pipeline']
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

    create_table = SqliteOperator(
        # This actually should be completely removed, and allow pd to handle the schema,
        # but leaving just so I can have the quick refresher on SQL in Airflow
        task_id = 'create_table',
        sqlite_conn_id = 'ins_sqlite_database',
        sql = r'''
            CREATE TABLE IF NOT EXISTS insurance_charges (
                id          INTEGER PRIMARY KEY,
                age         INTEGER,
                sex         TEXT CHECK( sex IN ('male','female') ),
                bmi         REAL,
                children    INTEGER,
                smoker      TEXT CHECK( smoker IN ('yes','no') ),
                region      TEXT CHECK( region IN ('southwest', 'southeast', 'northwest', 'northeast') ),
                charges     REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        '''
    )

    insert_values = PythonOperator(
        task_id = 'insert_values',
        python_callable = insert_values,
    )

    display_result = SqliteOperator(
        task_id = 'display_result',
        sqlite_conn_id = 'ins_sqlite_database',
        sql= r'''SELECT * FROM insurance_charges''',
        do_xcom_push = True
    )

extract_data >> remove_null_values >> [groupby_smoker, groupby_region, create_table]
create_table >> insert_values >> display_result