from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    return 'Hello world :)'


dag = DAG('hello_world',
          description='Hello world DAG',
          schedule_interval='0 17 * * *',
          start_date=datetime(2019, 2, 10)
          )

dummy_task = DummyOperator(task_id='dummy_task_id',
                           retries=5,
                           dag=dag)

python_task = PythonOperator(task_id='hello_task_id',
                             python_callable=print_hello,
                             dag=dag)

dummy_task >> python_task
