from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import pathlib
import gspread

service_account_path = pathlib.Path(
    './extras/analytics-google-service-account.json')

spreadsheet_id = '1oTdca4ldaEFXaye6Kxu8iPmyWU0NZTjTIgIBjZ6ddbk'

column_definitions = {
    'default': 'VARCHAR(128) ENCODE ZSTD',
    'start_date': 'TIMESTAMP',
    'experiment_id': 'VARCHAR(36) ENCODE ZSTD DISTKEY',
    'description': 'VARCHAR(400) ENCODE ZSTD',
    'hypothesis': 'VARCHAR(400) ENCODE ZSTD',
    'duration': 'INT',
}

sort_keys = ['start_date']


def get_control_panel_values(ts, **kwargs):
    gc = gspread.service_account(filename=service_account_path.as_posix())
    sh = gc.open_by_key(spreadsheet_id)

    ws = sh.get_worksheet(0)
    records = ws.get_all_records()

    if len(records) > 0:
        # Get column names from the first record
        example_record = records[0]
        columns = example_record.keys()
        task_instance = kwargs['task_instance']
        task_instance.xcom_push(
            key='experiment_platform_columns', value=columns)

    return records


def form_control_panel_query(ts, **kwargs):
    task_instance = kwargs['task_instance']
    columns = task_instance.xcom_pull(key='experiment_platform_columns')

    l = []
    for column in columns:
        column = column[:column.index('(')].strip().lower()
        column = column.replace(' ', '_')
        l.append('%s %s' % (column, column_definitions.get(
            column, column_definitions['default'])))
    query = '''
    CREATE TABLE IF NOT EXISTS ab_platform.experiment_control_panel (
        %s
    )
    COMPOUND SORT KEY (%s)
    ''' % (','.join(l), ','.join(sort_keys))
    return query


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('experimental_control_panel_ingest',
         start_date=datetime(2020, 6, 10),
         max_active_runs=1,
         schedule_interval=timedelta(minutes=10),
         default_args=default_args,
         ) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    get_control_panel_task = PythonOperator(
        task_id='get_control_panel_values',
        python_callable=get_control_panel_values,
        provide_context=True
    )

    form_control_panel_query_task = PythonOperator(
        task_id='form_control_panel_query',
        python_callable=form_control_panel_query,
        provide_context=True
    )

    start_task >> get_control_panel_task >> form_control_panel_query_task
    pass
