import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from experimental_platform_modules import util


MANUAL_POPULATION_CHECKSUM_TABLE = 'ab_platform.manual_population_checksums'


def get_existing_populations_checksums(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    SELECT * FROM {};
    '''.format(MANUAL_POPULATION_CHECKSUM_TABLE)
    records = pg_hook.get_records(query)
    return dict(records)


def get_manual_population_checksums(ts, **kwargs):
    d = {}
    parent_dir = './manual_populations/continuous_update'
    for item in os.listdir(parent_dir):
        item_path = os.path.join(parent_dir, item)
        with open(item_path, 'r') as f:
            s = f.read()

        checksum = util.fast_checksum(s)
        d[item_path] = checksum
    return d


def find_changed_checksums(ts, **kwargs):
    task_instance = kwargs['task_instance']
    existing_population_checksums = task_instance.xcom_pull(
        task_ids='get_existing_populations_checksums'
    )
    manual_population_checksums = task_instance.xcom_pull(
        task_ids='get_manual_population_checksums'
    )

    missing_populations = []

    for path, checksum in manual_population_checksums.items():
        print('Checking path {}'.format(path))
        existing_checksum = existing_population_checksums.get(path)
        if not existing_checksum:
            print('New population file detected {}'.format(path))
            missing_populations.append(path)
        elif existing_checksum != checksum:
            print('{} checksum doesn\'t match'.format(path))
            print('Existing Checksum: {} | New Checksum: {}'.format(
                existing_checksum, checksum))
            missing_populations.append(path)

    return missing_populations


def run_missing_populations(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    task_instance = kwargs['task_instance']
    missing_populations = task_instance.xcom_pull(
        task_ids='find_changed_checksums'
    )

    for population_path in missing_populations:
        with open(population_path, 'r') as f:
            query = f.read()
            print('Running query found in {}'.format(population_path))
        try:
            pg_hook.run(query)
            checksum = util.fast_checksum(query)
            checksum_query = '''
            begin;
            DELETE FROM {table_name} WHERE filename = '{filename}';
            INSERT INTO {table_name} VALUES ('{filename}', '{checksum}');
            commit;
            '''.format(**{
                'table_name': MANUAL_POPULATION_CHECKSUM_TABLE,
                'filename': population_path,
                'checksum': checksum,
            })
            pg_hook.run(checksum_query)
            print("Success!")
        except Exception as e:
            print("Error: {}".format(e))


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
with DAG('experimental_population_incremental_updater',
         start_date=datetime(2020, 11, 24, 17),  # Starts at 5pm PST
         max_active_runs=1,
         catchup=False,
         schedule_interval='@hourly',
         default_args=default_args,
         on_success_callback=None,
         on_failure_callback=None,
         ) as dag:

    # Tasks for population
    start_task = DummyOperator(
        task_id='start'
    )

    get_existing_populations_checksums_task = PythonOperator(
        task_id='get_existing_populations_checksums',
        python_callable=get_existing_populations_checksums,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    get_manual_population_checksums_task = PythonOperator(
        task_id='get_manual_population_checksums',
        python_callable=get_manual_population_checksums,
        provide_context=True
    )

    find_changed_checksums_task = PythonOperator(
        task_id='find_changed_checksums',
        python_callable=find_changed_checksums,
        provide_context=True
    )

    run_missing_populations_task = PythonOperator(
        task_id='run_missing_populations',
        python_callable=run_missing_populations,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    start_task >> [get_existing_populations_checksums_task,
                   get_manual_population_checksums_task] >> \
        find_changed_checksums_task >> run_missing_populations_task
