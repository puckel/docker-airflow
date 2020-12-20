from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'Neylson Crepalde',
    "depends_on_past": False,
    "start_date": datetime(2020, 11, 14, 23, 50),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "treino-02", 
    description="Get Titanic data from internet and calculate mean age",
    default_args=default_args, 
    schedule_interval=timedelta(minutes=5)
)

get_data = BashOperator(
    task_id="get-data",
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/train.csv',
    dag=dag
)

def calculate_mean_age():
    df = pd.read_csv('~/train.csv')
    med = df.Age.mean()
    return med

task_calculate_mean = PythonOperator(
    task_id='calculate-mean-age',
    python_callable=calculate_mean_age,
    dag=dag
)

def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calculate-mean-age')
    print(f"Mean age is {value}")

task_mean_age = PythonOperator(
    task_id="say-mean-age",
    python_callable=print_age,
    provide_context=True,
    dag=dag
)

get_data >> task_calculate_mean >> task_mean_age