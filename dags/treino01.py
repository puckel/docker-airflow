from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Neylson Crepalde',
    "depends_on_past": False,
    "start_date": datetime(2020, 11, 14),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "treino-01",
    description="Básico de Bash operators e Python operators", 
    default_args=default_args, 
    schedule_interval=timedelta(minutes=1)
)

def hello_word():
    print("Hello Airflow from Python")


hello_bash = BashOperator(
    task_id="hello-bash",
    bash_command='echo "Hello Airflow from bash"',
    dag=dag
)

hello_python = PythonOperator(
    task_id='hello-python',
    python_callable=hello_word,
    dag=dag
)


hello_bash >> hello_python