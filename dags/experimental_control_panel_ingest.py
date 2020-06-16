from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook
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
    'description': 'VARCHAR(600) ENCODE ZSTD',
    'hypothesis': 'VARCHAR(600) ENCODE ZSTD',
    'duration': 'INT',
    'archived': 'BOOLEAN DEFAULT false',
    'archivedat': 'TIMESTAMP',
    'createdat': 'TIMESTAMP DEFAULT sysdate'
}

additional_columns = ['archived', 'archivedat', 'createdat']

# Order matters here
sort_keys = ['start_date']


def get_control_panel_values(ts, **kwargs):
    gc = gspread.service_account(filename=service_account_path.as_posix())
    sh = gc.open_by_key(spreadsheet_id)

    ws = sh.get_worksheet(0)
    records = ws.get_all_records()

    if len(records) > 0:
        # Get column names from the first record
        example_record = records[0]
        columns = list(example_record.keys())
        task_instance = kwargs['task_instance']
        task_instance.xcom_push(
            key='experiment_platform_columns', value=columns)

    return records


def _format_columns(columns):
    l = []
    for column in columns:
        endIndex = column.find('(') if column.find('(') != -1 else len(column)
        column = column[:endIndex].strip().lower()
        column = column.replace(' ', '_')
        l.append(column)
    return l


def form_control_panel_query(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    raw_columns = task_instance.xcom_pull(key='experiment_platform_columns')

    # Format the columns from the spreadsheet, add additional columns
    columns = _format_columns(raw_columns) + additional_columns
    task_instance.xcom_push(
        key='experiment_platform_formatted_columns', value=columns)

    formatted_columns = ['%s %s' % (column, column_definitions.get(
        column, column_definitions['default'])) for column in columns]

    query = '''
    CREATE TABLE IF NOT EXISTS ab_platform.experiment_control_panel (
        %s
    )
    COMPOUND SORTKEY (%s)
    ''' % (','.join(formatted_columns), ','.join(sort_keys))

    pg_hook.run(query)
    return query


def sync_control_panel_shape(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    described_columns = task_instance.xcom_pull(
        key='experiment_platform_formatted_columns')

    query = '''
    SELECT
        column_name
    FROM information_schema.columns
    WHERE
        table_name = 'experiment_control_panel' AND
        table_schema = 'ab_platform'
    '''
    records = pg_hook.get_records(query)
    column_names = [record[0] for record in records]

    # Only additive columns
    new_column_names = set(described_columns) - set(column_names)
    additive_columns = ['%s %s' % (column, column_definitions.get(
        column, column_definitions['default'])) for column in list(new_column_names)]

    alter_queries = ['ALTER TABLE ab_platform.experiment_control_panel ADD COLUMN %s' %
                     column_def for column_def in additive_columns]
    for q in alter_queries:
        pg_hook.run(q)


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
         catchup=False,
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
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    sync_control_panel_shape_task = PythonOperator(
        task_id='sync_control_panel_shape',
        python_callable=sync_control_panel_shape,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    start_task >> get_control_panel_task >> form_control_panel_query_task >> sync_control_panel_shape_task
    pass
