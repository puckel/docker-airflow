from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'git_sync',
    default_args=default_args,
    catchup=False,
    schedule_interval='*/5 * * * *')

BashOperator(task_id='git-sync', bash_command='git-sync.sh ', dag=dag)
