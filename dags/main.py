from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
from spotify_transf import read_csv, transform_csv
from grammy_transf import read_db, transform_db
from merge import merge


import sys 
import os
#sys.path.append(os.path.abspath("/opt/airflow/dags/dag_connections/"))
#from etl import extract, transform, load



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 6),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'workshop_dag_etl',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:

    merge_taks = PythonOperator(
        task_id='merge',
        python_callable= merge,
        provide_context = True,
    )

    read_csv_taks = PythonOperator(
        task_id='read_csv',
        python_callable= read_csv,
        provide_context = True,
        )
    
    transformation_csv = PythonOperator(
        task_id='transformation_csv',
        python_callable= transform_csv,
        provide_context = True,
        )
    
    read_db_taks = PythonOperator(
        task_id='read_db',
        python_callable= read_db,
        provide_context = True,
        )
    
    transformation_db = PythonOperator(
        task_id='transformation_db',
        python_callable= transform_db,
        provide_context = True,
        )
    


    read_csv_taks >> transformation_csv >> merge_taks
    read_db_taks >> transformation_db >> merge_taks
    