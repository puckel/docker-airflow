"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from operators.get_stations_api_operator import GetStationsAPIOperator
from operators.get_hydrology_api_operator import GetHydrologyAPIOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "max_active_runs": 1,
    "start_date": datetime(2020, 9, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("tutorial", default_args=default_args) #, schedule_interval=timedelta(1)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = GetStationsAPIOperator(task_id="Get_Stations_from_API", dag=dag)

t2 = GetHydrologyAPIOperator(task_id="Get_Hydrology_Measures_from_API", dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

t2.set_upstream(t1)
t3.set_upstream(t1)
