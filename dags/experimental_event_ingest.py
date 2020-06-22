from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

CONTROL_PANEL_TABLE = 'ab_platform.experiment_control_panel'
EXPERIMENTAL_METADATA_TABLE = 'ab_platform.ingestion_run'


def create_run_metadata_table(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS %s (
        run_id VARCHAR(36) ENCODE ZSTD DISTKEY,
        start_ts TIMESTAMP,
        end_ts TIMESTAMP,
        status VARCHAR(128) ENCODE ZSTD
    )
    COMPOUND SORTKEY(experiment_id, end_ts, start_ts)
    ;
    ''' % (EXPERIMENTAL_METADATA_TABLE,)
    pg_hook.run(query)


def get_active_experiment_ids(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    SELECT
        experiment_id
    FROM %s
    WHERE
        archived = false
    '''
    records = pg_hook.get_records(query)
    experiment_ids = [record[0] for record in records]
    return experiment_ids


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('experimental_event_ingest',
         start_date=datetime(2020, 6, 10),
         max_active_runs=1,
         catchup=False,
         schedule_interval=timedelta(minutes=10),
         default_args=default_args,
         ) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    create_run_metadata_task = PythonOperator(
        task_id='create_run_metadata_table',
        python_callable=create_run_metadata_table,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True,
    )

    get_active_experiment_task = PythonOperator(
        task_id='get_active_experiments',
        python_callable=get_active_experiment_ids,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )
    start_task >> [create_run_metadata_task, get_active_experiment_task]
