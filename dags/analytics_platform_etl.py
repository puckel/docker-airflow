from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook

from datetime import timedelta, datetime

import pprint
import uuid


RUN_STATE_IN_PROGRESS = 'in_progress'
RUN_STATE_FAIL = 'fail'
RUN_STATE_SUCCESS = 'success'


def failure_callback(ctx):
    print("FAILED")
    # Add PD

def on_success_callback(ctx):
    print(ctx)


dag = DAG("analytics_platform_etl",
          description="Take the data from logs.analytics_platform_event and put them into the correct tables",
          schedule_interval=timedelta(minutes=30),
          concurrency=5,
          dagrun_timeout=timedelta(minutes=60),
          max_active_runs=1,
          catchup=False,
          start_date=datetime(2020, 1, 13),
          on_failure_callback=failure_callback,
          on_success_callback=on_success_callback)

def generate_run_record(conn_id, ts, **kwargs):
    run_id = str(uuid.uuid4())
    pg_hook = PostgresHook(conn_id)
    query = '''
        INSERT INTO analytics_platform.etl_run_record VALUES (
            '%s', '%s', timestamp '%s', NULL, NULL
        )
    ''' % (run_id, RUN_STATE_IN_PROGRESS, ts)
    pg_hook.run(query)
    return run_id

generate_run_record_task = PythonOperator(
    task_id='generate_run_record',
    op_kwargs={'conn_id': 'analytics_redshift'},
    provide_context=True,
    python_callable=generate_run_record,
    dag=dag
)

def get_last_successful_run_pull_time(conn_id, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
        SELECT
            recordqueryts
        FROM
            analytics_platform.etl_run_record
        WHERE
            state = %s
        ORDER BY
            recordqueryts desc
        limit 1
    ''' % RUN_STATE_SUCCESS
    records = pg_hook.get_records(query)
    timestamps = [r[0] for r in records]
    ts = timestamps[0] if timestamps else None
    return ts

get_last_successful_run_pull_time_task = PythonOperator(
    task_id='get_last_successful_run_pull_time',
    op_kwargs={'conn_id': 'analytics_redshift'},
    python_callable=get_last_successful_run_pull_time,
    dag=dag
)

def get_stop_list(conn_id, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
        SELECT tablename from analytics_platform.analytics_platform_metadata where active = false
    '''
    records = pg_hook.get_records(query)
    l = [r[0] for r in records]
    return records

get_stop_list_task = PythonOperator(
    task_id="get_stop_list",
    op_kwargs={'conn_id': 'analytics_redshift'},
    python_callable=get_stop_list,
    dag=dag
)

def select_analytics_events(ts, conn_id, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
        SELECT
            sessionId, entityId, createdAt, eventName, eventValue, userType, appVersion, metadata
        FROM
            logs.analytics_platform_event
        WHERE
            createdAt >= timestamp '%s' - interval '30 minutes'
    ''' % ts

    records = pg_hook.get_records(query)

    # Zip field names into each record
    l = [
        dict(zip(['sessionId', 'entityId', 'createdAt', 'eventName', 'eventValue', 'userType', 'appVersion', 'metadata'], r))
        for r in records
    ]
    return l

select_analytics_platform_events_task = PythonOperator(
    task_id="select_analytics_platform_events",
    provide_context=True,
    op_kwargs={'conn_id': 'analytics_redshift'},
    python_callable=select_analytics_events,
    dag=dag
)


def process_records(**kwargs):
    task_instance = kwargs['task_instance']
    records = task_instance.xcom_pull(task_ids='select_analytics_platform_events')
    stop_list = task_instance.xcom_pull(task_ids='get_stop_list')
    run_id = task_instance.xcom_pull(task_ids='generate_run_record')
    lastQueryTs = task_instance.xcom_pull(task_ids='get_last_successful_run_pull_time')

    for record in records:
        event_name = record['eventName']
        event_name_list = event_name.split('.')

        if len(event_name_list) < 2:
            print("EventName for record invalid, skipping record")
            pprint.pprint(record)
            continue

        tablename = event_name_list[1]
        if tablename in stop_list:
            print("record is part of stop list, skipping")
            pprint.pprint(record)
            continue

        if event_name_list[-1] == 'exposure' or event_name_list[-1] == 'conversion':
            event_name_list[0] = event_name_list[-1]
            event_name_list = event_name_list[:-1]

        record['logType'] = event_name_list[0] # have to do this to fix
        record['tableName'] = tablename
        record['qualifier'] = '.'.join(event_name_list[2:]) # because of magic, this doesn't get us an index error if len < 2

    return records


process_records_task = PythonOperator(
    task_id="process_records",
    provide_context=True,
    python_callable=process_records,
    dag=dag
)


def extract_table_names(**kwargs):
    task_instance = kwargs['task_instance']
    records = task_instance.xcom_pull(task_ids='select_analytics_platform_events')
    stop_list = task_instance.xcom_pull(task_ids='get_stop_list')
    tablename_set = set()

    for record in records:
        event_name = record['eventName']
        event_name_list = event_name.split('.')
        if len(event_name_list) < 2:
            print("EventName for record invalid, skipping record")
            pprint.pprint(record)
            continue

        tablename = event_name_list[1]
        if tablename in stop_list:
            print("Tablename %s found in stop list, skipping" % tablename)
            continue

        tablename_set.add(tablename)

    return list(tablename_set)

extract_table_names_task = PythonOperator(
    task_id="extract_table_names",
    provide_context=True,
    python_callable=extract_table_names,
    dag=dag
)

def create_experiment_tables(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    tablenames = task_instance.xcom_pull(task_ids='extract_table_names')
    pg_hook = PostgresHook(conn_id)

    for tablename in tablenames:
        # Create Table
        query = '''
            CREATE TABLE IF NOT EXISTS analytics_platform.%s (
                sessionId varchar(100) encode zstd,
                entityId varchar(100) encode zstd distkey,
                createdAt timestamp encode zstd,
                logType varchar(100) encode zstd,
                qualifier varchar(100) NULL encode zstd,
                eventValue varchar(100) NULL encode zstd,
                userType varchar(32) NULL encode zstd,
                appVersion varchar(20) NULL encode zstd,
                metadata varchar(5000) NULL encode zstd
            )
            diststyle key
            sortkey(createdAt, logType, qualifier, entityId)
        ''' % tablename
        pg_hook.run(query)

        # Update metadata
        query = '''
            SELECT * FROM analytics_platform.analytics_platform_metadata WHERE tableName = '%s'
        ''' % tablename

        records = pg_hook.get_records(query)
        if not records:
            query = '''
                INSERT INTO analytics_platform.analytics_platform_metadata (
                    tableName, createdAt, teamOwner, active, lastUpdated
                ) VALUES (
                    '%s', timestamp '%s', NULL, true, NULL
                )
            ''' % (tablename, ts)

        pg_hook.run(query)


create_experiment_tables_task = PythonOperator(
    task_id = 'create_experiment_tables',
    provide_context=True,
    op_kwargs={'conn_id': 'analytics_redshift'},
    python_callable=create_experiment_tables,
    dag=dag
)

def insert_records(conn_id, **kwargs):
    pg_hook = PostgresHook(conn_id)
    task_instance = kwargs['task_instance']
    records = task_instance.xcom_pull(task_ids='process_records')
    updated_tablenames = []
    insert_values_by_table = {}
    query = "begin;"

    for record in records:
        insert_values = insert_values_by_table.get(record['tableName'], [])

        values = '''('%(sessionId)s','%(entityId)s',timestamp '%(createdAt)s','%(logType)s','%(qualifier)s','%(eventValue)s','%(userType)s','%(appVersion)s','%(metadata)s')''' % record
        values.replace("'None'", "NULL") # Make sure we get rid of Nones
        insert_values.append(values)

        insert_values_by_table[record['tableName']] = insert_values

    for tablename,insert_values in insert_values_by_table.items():
        partial_query = '''
            insert into analytics_platform.%s values %s;
        ''' % (tablename, ','.join(insert_values))
        pprint.pprint(partial_query)

        query += partial_query
        updated_tablenames.append(tablename)
        #TODO run sql query
    query += "commit;"
    pg_hook.run(query)

    return updated_tablenames

insert_records_task = PythonOperator(
    task_id = 'insert_records',
    provide_context=True,
    op_kwargs={'conn_id': 'analytics_redshift'},
    python_callable=insert_records,
    dag=dag
)

generate_run_record_task >> [select_analytics_platform_events_task, get_stop_list_task, get_last_successful_run_pull_time_task] >> [process_records_task, extract_table_names_task]
extract_table_names_task >> create_experiment_tables_task
[process_records_task, create_experiment_tables_task] >> insert_records_task
