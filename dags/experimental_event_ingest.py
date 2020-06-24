from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from dateutil import parser

import time
import uuid

CONTROL_PANEL_TABLE = 'ab_platform.experiment_control_panel'
EXPERIMENTAL_METADATA_TABLE = 'ab_platform.ingestion_run'
AUDIENCE_METADATA_TABLE = 'ab_platform.audience_run'
AUDIENCE_MAPPING_TABLE = 'ab_platform.experiment_to_table_map'


def generate_run_uuid(**kwargs):
    return uuid.uuid4()


def create_run_metadata_table(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS %s (
        run_id VARCHAR(36) ENCODE ZSTD DISTKEY,
        start_ts TIMESTAMP,
        end_ts TIMESTAMP,
        event_count BIGINT,
        status VARCHAR(128) ENCODE ZSTD
    )
    COMPOUND SORTKEY(end_ts, start_ts)
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


def tag_run(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    start_ts = task_instance.xcom_pull(key='start_ts')
    end_ts = task_instance.xcom_pull(key='end_ts')
    run_uuid = task_instance.xcom_pull(task_ids='generate_run_uuid')

    query = '''
    INSERT INTO %s (run_id, start_ts, end_ts, event_count, status)
    VALUES('%s', '%s', '%s', 0, 'success')
    ''' % (EXPERIMENTAL_METADATA_TABLE, run_uuid, start_ts, end_ts)
    pg_hook.run(query)


# Audience run
def create_audience_run_metadata_table(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS %s (
        run_id VARCHAR(36) ENCODE ZSTD DISTKEY,
        status VARCHAR(128) ENCODE ZSTD,
        createdat TIMESTAMP DEFAULT SYSDATE
    )
    COMPOUND SORTKEY(createdat);
    ''' % AUDIENCE_METADATA_TABLE
    pg_hook.run(query)


def create_mapping_table(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS %s (
        experiment_id VARCHAR(36) ENCODE ZSTD DISTKEY,
        table_type VARCHAR(36) ENCODE ZSTD,
        table_name VARCHAR(256) ENCODE ZSTD
    );
    ''' % AUDIENCE_MAPPING_TABLE
    pg_hook.run(query)


def get_manually_mapped_tables(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    SELECT
        experiment_id,
        manual_population_override
    FROM %s
    WHERE
        archived = false and population_basis = 'Manual' and
        manual_population_override is not NULL
    ''' % CONTROL_PANEL_TABLE

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
        WHERE experiment_id='%(experiment_id)s' and table_type='population';
        INSERT INTO %(mapping_table)s (experiment_id, table_name, table_type)
        VALUES ('%(experiment_id)s', '%(table_name)s', 'population');
        commit;
        ''' % {
            'mapping_table': AUDIENCE_MAPPING_TABLE,
            'experiment_id': experiment_id,
            'table_name': table_name,
        }
        pg_hook.run(query)


def tag_population_run(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    run_uuid = task_instance.xcom_pull(task_ids='generate_run_uuid')

    query = '''
    INSERT INTO %s (run_id, status) VALUES ('%s', 'success')
    ''' % (AUDIENCE_METADATA_TABLE, run_uuid)
    pg_hook.run(query)


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
         ) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    generate_run_uuid_task = PythonOperator(
        task_id='generate_run_uuid',
        python_callable=generate_run_uuid,
        provide_context=True
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

    tag_run_task = PythonOperator(
        task_id='tag_run',
        python_callable=tag_run,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )
    cleanup_intermediate_tables_task = DummyOperator(
        task_id='cleanup_intermediate_tables'
    )

    end_ingestion_task = DummyOperator(
        task_id='end_ingestion'
    )

    # Tasks for audience
    start_audience_task = DummyOperator(
        task_id='start_audience'
    )

    create_audience_run_metadata_task = PythonOperator(
        task_id='create_audience_run_metadata_table',
        python_callable=create_audience_run_metadata_table,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    create_mapping_table_task = PythonOperator(
        task_id='create_mapping_table',
        python_callable=create_mapping_table,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    generate_automatic_audience_task = DummyOperator(
        task_id='generate_automatic_audience'
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

    tag_population_run_task = PythonOperator(
        task_id='tag_population_run',
        python_callable=tag_population_run,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    # DAG for event ingestion
    start_task >> [create_run_metadata_task,
                   get_active_experiment_task, generate_run_uuid_task]
    get_active_experiment_task >> generate_missing_event_tables_tasks
    create_run_metadata_task >> generate_start_and_end_ts_task >> generate_intermediate_events_task
    [generate_missing_event_tables_tasks,
        generate_intermediate_events_task] >> populate_experiment_events_task
    [populate_experiment_events_task,
        generate_run_uuid_task] >> tag_run_task >> end_ingestion_task >> start_audience_task
    [populate_experiment_events_task,
        generate_run_uuid_task] >> cleanup_intermediate_tables_task

    end_ingestion_task >> start_audience_task

    # DAG for the audience ingestion
    start_audience_task >> [
        create_audience_run_metadata_task, create_mapping_table_task]
    create_mapping_table_task >> [
        generate_automatic_audience_task, get_manually_mapped_tables_task] >> write_mappings_task

    [create_audience_run_metadata_task, write_mappings_task] >> tag_population_run_task

    # DAG for results calculation