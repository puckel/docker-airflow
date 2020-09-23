"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from operators.get_stations_api_operator import GetStationsAPIOperator
from operators.get_hydrology_api_operator import GetHydrologyAPIOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "max_active_runs": 1,
    "start_date": datetime(2020, 9, 10),
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

dag = DAG("Hydrology-Data-Project", default_args=default_args) #, schedule_interval=timedelta(1)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = GetStationsAPIOperator(
    task_id="Get_Stations_from_API",
    target_database={
        "database": "airflow",
        "table": "stations",
        "user": "airflow",
        "password": "airflow"
    },
    dag=dag)

t2 = GetHydrologyAPIOperator(task_id="Get_Hydrology_Measures_from_API",
                             source_database={
                                 "database": "airflow",
                                 "table": "stations",
                                 "user": "airflow",
                                 "password": "airflow"
                             },
                             target_database={
                                 "database": "airflow",
                                 "table": "measures",
                                 "user": "airflow",
                                 "password": "airflow"
                             },
                             provide_context=True,
                             date='{{ds}}',
                             dag=dag)

t2.set_upstream(t1)

