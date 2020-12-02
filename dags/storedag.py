from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import BashOperator
from airflow.operators.python_operator import PythonOperator

from datacleaner import data_cleaner

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2020, 12, 2),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG('store_dag',default_args=default_args,schedule_interval='@daily', catchup=False)

t1=BashOperator(task_id='check_file_exists', bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',
                retries=2, retry_delay=timedelta(seconds=15), dag=dag)

t2 = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)
