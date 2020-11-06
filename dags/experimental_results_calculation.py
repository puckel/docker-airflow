import statsd

from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.sensors import ExternalTaskSensor


from datetime import datetime, timedelta
from dateutil import parser
from experimental_platform_modules import result_calculator

import os
import uuid

RESULTS_METADATA_TABLE = 'ab_platform.results_run'


def _create_results_run_table(conn_id):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS {} (
        run_id VARCHAR(36) ENCODE ZSTD distkey,
        status VARCHAR(128) ENCODE ZSTD,
        intermediate_results_date TIMESTAMP,
        createdat TIMESTAMP DEFAULT sysdate
    )
    COMPOUND SORTKEY(createdat)
    ;
    '''.format(RESULTS_METADATA_TABLE)
    pg_hook.run(query)


def _callback(state, ctx):
    task_instance = ctx['task_instance']
    conn_id = 'analytics_redshift'
    pg_hook = PostgresHook(conn_id)
    intermediate_results_run_date = task_instance.xcom_pull(
        key='intermediate_results_run_date'
    )
    run_id = uuid.uuid4()

    _create_results_run_table(conn_id)

    query = '''
    INSERT INTO {} (run_id, status, intermediate_results_date) VALUES
    ('{}', '{}', '{}'::TIMESTAMP)
    '''.format(RESULTS_METADATA_TABLE, run_id, state, intermediate_results_run_date.isoformat())
    pg_hook.run(query)

    conf = ctx['conf']
    if conf.getboolean('scheduler', 'statsd_on'):
        client = statsd.StatsClient(
            host=conf.get('scheduler', 'statsd_host'),
            port=conf.get('scheduler', 'statsd_port'),
            prefix=conf.get('scheduler', 'statsd_prefix'),
        )
        client.incr('results_dag.%s' % state, 1)


def success_callback(ctx):
    _callback('success', ctx)


def failure_callback(ctx):
    _callback('failure', ctx)


def get_date_to_calculate(ts, **kwargs):
    # Get the last days worth of stuff
    # Use this instead of the provided 'ds' so we can do some date operations
    task_instance = kwargs['task_instance']
    dt = parser.parse(ts)
    yesterday = dt.date() - timedelta(days=1)
    task_instance.xcom_push(
        key='intermediate_results_run_date', value=yesterday)


def get_active_experiment_and_population_map(analytics_conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    yesterday = task_instance.xcom_pull(
        key='intermediate_results_run_date')
    return result_calculator.get_active_experiment_and_population_map(
        analytics_conn_id, yesterday)


def create_intermediate_results_table(frontend_conn_id, ts, **kwargs):
    result_calculator.create_intermediate_results_table(frontend_conn_id)


def calculate_intermediate_results(analytics_conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    yesterday = task_instance.xcom_pull(
        key='intermediate_results_run_date')

    experiment_to_population_map = task_instance.xcom_pull(
        task_ids='get_active_experiment_and_population_map'
    )

    return result_calculator.calculate_intermediate_result_for_day(analytics_conn_id, yesterday, experiment_to_population_map, timeout=True)


def insert_intermediate_records(frontend_conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    records = task_instance.xcom_pull(
        task_ids='calculate_intermediate_results'
    )

    result_calculator.insert_intermediate_records(frontend_conn_id, records)


def calculate_results(frontend_conn_id, ts, **kwargs):
    result_calculator.calculate_results(frontend_conn_id)
    print("Done writing results to RDS")


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

default_task_kwargs = {
    'analytics_conn_id': 'analytics_redshift',
    'frontend_conn_id': 'ab_platform_frontend',
}

with DAG('experimental_results_calculator',
         start_date=datetime(2020, 6, 25, 17),  # Starts at 5pm PST
         max_active_runs=1,
         catchup=False,
         schedule_interval='@daily',
         default_args=default_args,
         on_failure_callback=failure_callback,
         on_success_callback=success_callback,
         ) as dag:

    # start_task = ExternalTaskSensor(
    #     task_id="start",
    #     external_dag_id="experiment_population_creation"
    # )

    start_task = DummyOperator(
        task_id='start'
    )
    get_date_to_calculate_task = PythonOperator(
        task_id='get_date_to_calculate',
        python_callable=get_date_to_calculate,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    create_intermediate_results_table_task = PythonOperator(
        task_id='create_intermediate_results_table',
        python_callable=create_intermediate_results_table,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    get_active_experiment_and_population_map_task = PythonOperator(
        task_id='get_active_experiment_and_population_map',
        python_callable=get_active_experiment_and_population_map,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    calculate_intermediate_results_task = PythonOperator(
        task_id='calculate_intermediate_results',
        python_callable=calculate_intermediate_results,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    insert_intermediate_records_task = PythonOperator(
        task_id='insert_intermediate_results',
        python_callable=insert_intermediate_records,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    calculate_results_task = PythonOperator(
        task_id='calculate_results',
        python_callable=calculate_results,
        op_kwargs=default_task_kwargs,
        provide_context=True,
    )

    start_task >> [get_date_to_calculate_task,
                   create_intermediate_results_table_task] >> \
        get_active_experiment_and_population_map_task >> \
        calculate_intermediate_results_task >> insert_intermediate_records_task >> \
        calculate_results_task
