from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from dateutil import parser

import time
import uuid

CONTROL_PANEL_TABLE = 'ab_platform.experiment_control_panel'
EXPERIMENTAL_METADATA_TABLE = 'ab_platform.ingestion_run'


def _create_run_metadata_table(conn_id):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS {} (
        run_id VARCHAR(36) ENCODE ZSTD DISTKEY,
        start_ts TIMESTAMP,
        end_ts TIMESTAMP,
        event_count BIGINT,
        status VARCHAR(128) ENCODE ZSTD
    )
    COMPOUND SORTKEY(end_ts, start_ts)
    ;
    '''.format(EXPERIMENTAL_METADATA_TABLE,)
    pg_hook.run(query)


def _callback(state, ctx):
    task_instance = ctx['task_instance']
    conn_id = 'analytics_redshift'
    pg_hook = PostgresHook(conn_id)
    start_ts = task_instance.xcom_pull(key='start_ts')
    end_ts = task_instance.xcom_pull(key='end_ts')
    run_uuid = uuid.uuid4()
    _create_run_metadata_table(conn_id)

    query = '''
    INSERT INTO {} (run_id, start_ts, end_ts, event_count, status)
    VALUES('{}', '{}', '{}', 0, '{}')
    '''.format(EXPERIMENTAL_METADATA_TABLE, run_uuid, start_ts, end_ts, state)
    pg_hook.run(query)


def success_callback(ctx):
    _callback('success', ctx)


def failure_callback(ctx):
    _callback('failure', ctx)


def get_active_experiment_ids(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    SELECT
        experiment_id
    FROM %s
    WHERE
        archived = false
    ''' % CONTROL_PANEL_TABLE
    records = pg_hook.get_records(query)
    experiment_ids = [record[0] for record in records]
    return experiment_ids


def generate_start_and_end_ts(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    dt = parser.parse(ts)

    query = '''
    SELECT
        max(end_ts)
    FROM %s
    ''' % EXPERIMENTAL_METADATA_TABLE

    records = pg_hook.get_records(query)
    # Will return none if nothing matches the query
    start_ts = [record[0] for record in records][0]
    if start_ts:
        task_instance.xcom_push(key='start_ts', value=start_ts)
    else:
        task_instance.xcom_push(
            key='start_ts', value=dt - timedelta(minutes=10))

    task_instance.xcom_push(key='end_ts', value=dt)


# TODO (Felix): Make this actually execute
def generate_intermediate_events(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    # pg_hook = PostgresHook(conn_id)
    start_ts = task_instance.xcom_pull(
        key='start_ts'
    )
    end_ts = task_instance.xcom_pull(
        key='end_ts'
    )
    intermediate_table_name = 'ab_platform.intermediate_events_%s' % str(
        int(start_ts.timestamp()))

    task_instance.xcom_push(
        key='intermediate_events_table_name', value=intermediate_table_name)

    query = '''
    begin;
    CREATE TABLE %(intermediate_table_name)s (LIKE logs.product_event_raw);
    INSERT INTO %(intermediate_table_name)s
    SELECT *
    FROM logs.product_event_raw
    WHERE
        createdat >= %(start_ts)s AND createdat < %(end_ts)s AND
        experiments IS NOT NULL;

    --This part should only come into play at time boundaries
    INSERT INTO %(intermediate_table_name)s
    SELECT *
    FROM logs.product_event_last_6_months
    WHERE
        createdat >= %(start_ts)s AND createdat < %(end_ts)s AND
        experiments IS NOT NULL;
    commit;
    ''' % {
        'intermediate_table_name': intermediate_table_name,
        'start_ts': start_ts.isoformat(),
        'end_ts': end_ts.isoformat()
    }
    print(query)


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
with DAG('experimental_event_ingest',
         start_date=datetime(2020, 6, 10),
         max_active_runs=1,
         catchup=False,
         schedule_interval=timedelta(minutes=10),
         default_args=default_args,
         on_success_callback=success_callback,
         on_failure_callback=failure_callback
         ) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    get_active_experiment_task = PythonOperator(
        task_id='get_active_experiments',
        python_callable=get_active_experiment_ids,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    generate_start_and_end_ts_task = PythonOperator(
        task_id='generate_start_and_end_ts',
        python_callable=generate_start_and_end_ts,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    generate_intermediate_events_task = PythonOperator(
        task_id='generate_intermediate_tasks',
        python_callable=generate_intermediate_events,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    generate_missing_event_tables_tasks = DummyOperator(
        task_id='generate_missing_event_tables'
    )

    populate_experiment_events_task = DummyOperator(
        task_id='populate_experiment_events'
    )

    cleanup_intermediate_tables_task = DummyOperator(
        task_id='cleanup_intermediate_tables'
    )

    trigger_population_creation_dag_task = TriggerDagRunOperator(
        task_id='trigger_population_creation_dag',
        trigger_dag_id='experimental_population_creation'
    )

    # DAG for event ingestion
    start_task >> [get_active_experiment_task, generate_start_and_end_ts_task]
    get_active_experiment_task >> generate_missing_event_tables_tasks
    generate_start_and_end_ts_task >> generate_intermediate_events_task
    [generate_missing_event_tables_tasks,
        generate_intermediate_events_task] >> populate_experiment_events_task
    populate_experiment_events_task >> cleanup_intermediate_tables_task >> trigger_population_creation_dag_task
