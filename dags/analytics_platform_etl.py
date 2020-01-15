from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook

from datetime import timedelta, datetime

import pprint

def failure_callback(ctx):
    print("FAILED")
    # Add PD

dag = DAG("analytics_platform_etl",
          description="Take the data from logs.analytics_platform_event and put them into the correct tables",
          schedule_interval=timedelta(minutes=30),
          concurrency=5,
          dagrun_timeout=timedelta(minutes=60),
          max_active_runs=1,
          catchup=False,
          start_date=datetime(2020, 1, 13),
          on_failure_callback=failure_callback)

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

t1 = PythonOperator(
    task_id="select_analytics_platform_events",
    provide_context=True,
    op_kwargs={'conn_id': 'analytics_redshift'},
    python_callable=select_analytics_events,
    dag=dag
)

def _extract_table_name(record):
    event_name = record['eventName']
    event_name_list = event_name.split('.')

    if len(event_name_list) < 2:
        return

    return event_name_list[1]

def process_records(**kwargs):
    task_instance = kwargs['task_instance']
    records = task_instance.xcom_pull(task_ids='select_analytics_platform_events')

    for record in records:
        print(record['eventName'])



t2 = PythonOperator(
    task_id="process_records",
    provide_context=True,
    python_callable=process_records,
    dag=dag
)

def extract_table_names(**kwargs):
    task_instance = kwargs['task_instance']
    records = task_instance.xcom_pull(task_ids='select_analytics_platform_events')
    tablename_set = set()

    for record in records:
        tablename = _extract_table_name(record)
        if not tablename:
            print("EventName for record invalid, skipping record")
            pprint.pprint(record)
            continue
        tablename_set.add(tablename)

    return list(tablename_set)

t3 = PythonOperator(
    task_id="extract_table_names",
    provide_context=True,
    python_callable=extract_table_names,
    dag=dag
)


t1 >> [t2, t3]
