from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

import pathlib
import os

CONTROL_PANEL_TABLE = 'ab_platform.experiment_control_panel'
POPULATION_MAPPING_TABLE = 'ab_platform.experiment_to_population_map'
EXPERIMENT_INTERMEDIATE_RESULTS_TABLE = 'ab_platform.experiment_results_intermediate'
RESULTS_METADATA_TABLE = 'ab_platform.results_run'


def get_active_experiment_and_population_map(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    SELECT
        a.experiment_id,
        a.population_kind,
        b.table_name
    FROM %s a JOIN %s b ON a.experiment_id = b.experiment_id
    WHERE getdate() > start_date and archived = false;
    ''' % (CONTROL_PANEL_TABLE, POPULATION_MAPPING_TABLE)
    records = pg_hook.get_records(query)
    experiment_id_to_table_mapping = {}
    for experiment_id, population_kind, table_name in records:
        experiment_id_to_table_mapping[experiment_id] = {
            'population_table_name': table_name,
            'population_type': population_kind,
        }
    return experiment_id_to_table_mapping


def create_intermediate_results_table(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    create table if not exists %s
    (
        experiment_id varchar(36) not null distkey,
        variant varchar(128) not null,
        metric_name varchar(128) not null,
        metric_type varchar(128) not null,
        segment varchar(128) not null,
        day timestamp not null,
        numerator integer encode az64,
        denominator integer encode az64,
        mean double precision,
        standard_deviation double precision
    )
    diststyle key
    compound sortkey(day, experiment_id);
    ''' % EXPERIMENT_INTERMEDIATE_RESULTS_TABLE
    pg_hook.run(query)


def create_results_run_table(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS %s (
        run_id VARCHAR(36) ENCODE ZSTD distkey,
        status VARCHAR(128) ENCODE ZSTD,
        createdat TIMESTAMP DEFAULT sysdate
    )
    COMPOUND SORTKEY(createdat)
    ;
    ''' % RESULTS_METADATA_TABLE
    pg_hook.run(query)


def calculate_intermediate_results(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    experiment_to_population_map = task_instance.xcom_pull(
        task_ids='get_active_experiment_and_population_map'
    )

    population_types = set([population_metadata['population_type']
                            for population_metadata in experiment_to_population_map.values()])

    population_templates = {}
    for population_type in population_types:
        population_templates[population_type] = {}
        parent_dir = os.path.join('./queries', population_type.lower())
        for item in os.listdir(parent_dir):
            item_path = os.path.join(parent_dir, item)
            metric_name = item.split('.')[0]
            with open(item_path, 'r') as f:
                s = f.read()
                population_templates[population_type][metric_name] = s

    print(population_templates)


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG('experimental_results_calculation',
         start_date=datetime(2020, 6, 25, 17),  # Starts at 5pm PST
         max_active_runs=1,
         catchup=True,
         schedule_interval=timedelta(days=1),
         default_args=default_args,
         ) as dag:

    default_task_kwargs = {
        'conn_id': 'analytics_redshift'
    }

    start_task = DummyOperator(
        task_id='start'
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

    create_results_run_table_task = PythonOperator(
        task_id='create_results_run',
        python_callable=create_results_run_table,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    calculate_intermediate_results_task = PythonOperator(
        task_id='calculate_intermediate_results',
        python_callable=calculate_intermediate_results,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    start_task >> [get_active_experiment_and_population_map_task,
                   create_intermediate_results_table_task,
                   create_results_run_table_task]

    [get_active_experiment_and_population_map_task,
        create_intermediate_results_table_task] >> calculate_intermediate_results_task
