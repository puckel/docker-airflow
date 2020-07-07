from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.sql_sensor import SqlSensor

from datetime import datetime, timedelta

import uuid

CONTROL_PANEL_TABLE = 'ab_platform.experiment_control_panel'
POPULATION_METADATA_TABLE = 'ab_platform.population_run'
POPULATION_MAPPING_TABLE = 'ab_platform.experiment_to_population_map'


def _create_population_run_metadata_table(conn_id):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS {} (
        run_id VARCHAR(36) ENCODE ZSTD DISTKEY,
        status VARCHAR(128) ENCODE ZSTD,
        createdat TIMESTAMP DEFAULT SYSDATE
    )
    COMPOUND SORTKEY(createdat);
    '''.format(POPULATION_METADATA_TABLE)
    pg_hook.run(query)


def _callback(state, ctx):
    conn_id = 'analytics_redshift'
    pg_hook = PostgresHook(conn_id)
    run_uuid = uuid.uuid4()

    # Create metadata table if it doesn't exist
    _create_population_run_metadata_table(conn_id)

    query = '''
    INSERT INTO {} (run_id, status) VALUES ('{}', '{}')
    '''.format(POPULATION_METADATA_TABLE, run_uuid, state)
    pg_hook.run(query)


def success_callback(ctx):
    _callback('success', ctx)


def failure_callback(ctx):
    _callback('failure', ctx)


def create_mapping_table(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS {} (
        experiment_id VARCHAR(36) ENCODE ZSTD DISTKEY,
        table_name VARCHAR(256) ENCODE ZSTD
    );
    '''.format(POPULATION_MAPPING_TABLE)
    pg_hook.run(query)


def get_manually_mapped_tables(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    SELECT
        experiment_id,
        manual_population_override
    FROM {}
    WHERE
        archived = false and population_basis = 'Manual' and
        manual_population_override is not NULL
    '''.format(CONTROL_PANEL_TABLE)

    records = pg_hook.get_records(query)
    d = dict(records)
    return d


def write_mappings(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    manually_mapped_tables = task_instance.xcom_pull(
        task_ids='get_manually_mapped_tables')

    for experiment_id, table_name in manually_mapped_tables.items():
        query = '''
        begin;
        DELETE FROM %(mapping_table)s
        WHERE experiment_id='%(experiment_id)s';
        INSERT INTO %(mapping_table)s (experiment_id, table_name)
        VALUES ('%(experiment_id)s', '%(table_name)s');
        commit;
        ''' % {
            'mapping_table': POPULATION_MAPPING_TABLE,
            'experiment_id': experiment_id,
            'table_name': table_name,
        }
        pg_hook.run(query)


event_task_sensor = SqlSensor(
    task_id='event_task_sensor',
    conn_id='airflow_db',
    sql='''
    SELECT count(*)
    FROM dag_run
    WHERE
        execution_date > getdate() - interval '10 minutes' and
        dag_id = 'experimental_event_ingest' and
        state = 'success'
    '''
)

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Note this is a "dummy" DAG for now.
with DAG('experimental_population_creation',
         start_date=datetime(2020, 6, 10),
         max_active_runs=1,
         catchup=False,
         schedule_interval=timedelta(minutes=10),
         default_args=default_args,
         on_success_callback=success_callback,
         on_failure_callback=failure_callback,
         ) as dag:

    # Tasks for population
    start_task = DummyOperator(
        task_id='start'
    )

    create_mapping_table_task = PythonOperator(
        task_id='create_mapping_table',
        python_callable=create_mapping_table,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    generate_automatic_population_task = DummyOperator(
        task_id='generate_automatic_population'
    )

    get_manually_mapped_tables_task = PythonOperator(
        task_id='get_manually_mapped_tables',
        python_callable=get_manually_mapped_tables,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    write_mappings_task = PythonOperator(
        task_id='write_mappings',
        python_callable=write_mappings,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    # DAG for the population ingestion
    event_task_sensor >> start_task >> create_mapping_table_task >> [
        generate_automatic_population_task, get_manually_mapped_tables_task] >> write_mappings_task
