from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook
from datetime import datetime, timedelta
from dateutil import parser

import pathlib
import gspread

CONTROL_PANEL_TABLE = 'ab_platform.experiment_control_panel'

service_account_path = pathlib.Path(
    './extras/analytics-google-service-account.json')

spreadsheet_id = '1oTdca4ldaEFXaye6Kxu8iPmyWU0NZTjTIgIBjZ6ddbk'

column_definitions = {
    'default': 'VARCHAR(128) ENCODE ZSTD',
    'start_date': 'TIMESTAMP',
    'experiment_id': 'VARCHAR(36) ENCODE ZSTD DISTKEY',
    'description': 'VARCHAR(600) ENCODE ZSTD',
    'hypothesis': 'VARCHAR(600) ENCODE ZSTD',
    'duration': 'INT DEFAULT 0',
    'end_result_calculation': 'INT DEFAULT 0',
    'archived': 'BOOLEAN DEFAULT false',
    'archivedat': 'TIMESTAMP',
}

additional_columns = ['archived', 'archivedat']

# Order matters here
sort_keys = ['start_date']


def _get_column_mapping(columns):
    d = {}

    for column in columns:
        endIndex = column.find('(') if column.find('(') != -1 else len(column)
        new_column = column[:endIndex].strip().lower().replace(' ', '_')
        d[column] = new_column
    return d


def get_control_panel_values(ts, **kwargs):
    gc = gspread.service_account(filename=service_account_path.as_posix())
    sh = gc.open_by_key(spreadsheet_id)

    ws = sh.get_worksheet(0)
    records = ws.get_all_records()

    if len(records) > 0:
        # Get column names from the first record
        example_record = records[0]
        columns = list(example_record.keys())
        column_mapping = _get_column_mapping(columns)

        task_instance = kwargs['task_instance']
        task_instance.xcom_push(
            key='experiment_platform_column_mapping', value=column_mapping)

    return records


def create_control_panel_table(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    column_mapping = task_instance.xcom_pull(
        key='experiment_platform_column_mapping')

    # Format the columns from the spreadsheet, add additional columns
    columns = list(column_mapping.values()) + additional_columns
    task_instance.xcom_push(
        key='experiment_platform_formatted_columns', value=columns)

    formatted_columns = ['%s %s' % (column, column_definitions.get(
        column, column_definitions['default'])) for column in columns]

    query = '''
    CREATE TABLE IF NOT EXISTS %s (
        %s
    )
    COMPOUND SORTKEY (%s)
    ''' % (CONTROL_PANEL_TABLE, ','.join(formatted_columns), ','.join(sort_keys))

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


def sort_records(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    records = task_instance.xcom_pull(task_ids='get_control_panel_values')

    # Sheet keys
    id_name = 'Experiment ID (AUTO)'

    ids = [record[id_name] for record in records]
    query = '''
    SELECT
        experiment_id
    FROM ab_platform.experiment_control_panel
    '''
    experiment_records = pg_hook.get_records(query)
    experiment_ids = [record[0] for record in experiment_records]
    missing_ids = set(ids) - set(experiment_ids)
    deactivated_ids = set(experiment_ids) - set(ids)
    active_existing_ids = set(ids) - set(missing_ids) - \
        set(deactivated_ids)

    task_instance.xcom_push(
        key="missing_experiment_ids", value=missing_ids)
    task_instance.xcom_push(
        key="to_archive_experiment_ids", value=deactivated_ids)
    task_instance.xcom_push(
        key="active_existing_experiment_ids", value=active_existing_ids)


def _get_insert_query(record, column_mapping):
    column_names = []
    values = []
    for k, v in record.items():
        column = column_mapping[k]
        column_names.append(column)
        if not v:
            values.append('null')
        elif type(v) == str:
            values.append("'%s'" % v)
        else:
            values.append(str(v))

    value_str = ','.join(values)
    insert_query = '''
    INSERT INTO ab_platform.experiment_control_panel (%s)
    values (%s)
    ''' % (','.join(column_names), value_str)
    return insert_query


def insert_new_records(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    records = task_instance.xcom_pull(task_ids='get_control_panel_values')
    id_name = 'Experiment ID (AUTO)'
    column_mapping = task_instance.xcom_pull(
        key='experiment_platform_column_mapping')

    missing_ids = task_instance.xcom_pull(
        key="missing_experiment_ids")
    missing_records = [
        r for r in records if r[id_name] in missing_ids]

    for r in missing_records:
        experiment_id = r[id_name]
        insert_query = _get_insert_query(r, column_mapping)
        try:
            pg_hook.run(insert_query)
        except Exception as e:
            print("Could not insert id: %s" % experiment_id)
            print(e)
            continue


def archive_records(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)

    to_archive_ids = task_instance.xcom_pull(
        key="to_archive_experiment_ids")

    for i in to_archive_ids:
        update_query = '''
        UPDATE ab_platform.experiment_control_panel SET archived=true, archivedat=getdate()
        WHERE experiment_id='%s' and archived = false
        ''' % (i)
        pg_hook.run(update_query)


# For now just delete and reinsert all of these
def update_existing_records(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    records = task_instance.xcom_pull(
        task_ids='get_control_panel_values')
    id_name = 'Experiment ID (AUTO)'
    column_mapping = task_instance.xcom_pull(
        key='experiment_platform_column_mapping')

    existing_ids = task_instance.xcom_pull(
        key="active_existing_experiment_ids")
    existing_records = [
        r for r in records if r[id_name] in existing_ids]

    # Avoid udpates where we can
    for r in existing_records:
        experiment_id = r[id_name]
        delete_query = '''
        DELETE FROM ab_platform.experiment_control_panel WHERE experiment_id = '%s'
        ''' % (experiment_id)
        insert_query = _get_insert_query(r, column_mapping)
        full_query = '''
        begin;
        %s;
        %s;
        commit;
        ''' % (delete_query, insert_query)

        pg_hook.run(full_query)


def finish_experiments(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    update_query = '''
    UPDATE ab_platform.experiment_control_panel
    SET
        archived=true,
        archivedat=getdate()
    WHERE
        archived = false
        AND getdate() > DATEADD('days', COALESCE(duration, 0) + COALESCE(end_result_calculation, 0), start_date)
        AND decision is not null AND decision != ''
    '''
    pg_hook.run(update_query)


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

    create_control_panel_table_task = PythonOperator(
        task_id='create_control_panel_table',
        python_callable=create_control_panel_table,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    sync_control_panel_shape_task = PythonOperator(
        task_id='sync_control_panel_shape',
        python_callable=sync_control_panel_shape,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    sort_records_task = PythonOperator(
        task_id='sort_records',
        python_callable=sort_records,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    insert_new_record_task = PythonOperator(
        task_id='insert_new_records',
        python_callable=insert_new_records,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    archive_record_task = PythonOperator(
        task_id='archive_deleted_records',
        python_callable=archive_records,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    update_existing_record_task = PythonOperator(
        task_id='update_existing_records',
        python_callable=update_existing_records,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    finish_experiments_task = PythonOperator(
        task_id='finish_experiments',
        python_callable=finish_experiments,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    start_task >> get_control_panel_task >> create_control_panel_table_task >> sync_control_panel_shape_task >> sort_records_task
    sort_records_task >> [insert_new_record_task,
                          archive_record_task,
                          update_existing_record_task] >> finish_experiments_task
