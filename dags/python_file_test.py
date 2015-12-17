from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta


from worker import SimplePython

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

dag = DAG(dag_id='python_file_test', default_args=args)

run_this = PythonOperator(
    task_id='print',
    provide_context=True,
    python_callable=SimplePython.print_context,
    dag=dag)
