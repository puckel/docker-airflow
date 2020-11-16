import os
import statsd
import time

from airflow import DAG

from airflow.hooks import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

CONTROL_PANEL_TABLE = 'ab_platform.experiment_control_panel'
POPULATION_MAPPING_TABLE = 'ab_platform.experiment_to_population_map'


def gather_manually_mapped_tables(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    SELECT
        cp.experiment_id,
        pm.table_name
    FROM {} cp JOIN {} pm ON cp.experiment_id = pm.experiment_id
    WHERE cp.population_basis = 'Manual';
    '''.format(CONTROL_PANEL_TABLE, POPULATION_MAPPING_TABLE)
    records = pg_hook.get_records(query)
    d = dict(records)
    return d


def check_table_existence(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(conn_id)
    records = task_instance.xcom_pull(
        task_ids='gather_manually_mapped_tables')
    missing_experiments = []

    for experiment_id, table_name in records.items():
        query = '''
        SELECT EXISTS (
            SELECT *
            FROM information_schema.tables
            WHERE table_schema = 'ab_platform' AND
            table_name = '{}'
        ) as table_exists
        '''.format(table_name)
        exists_record = pg_hook.get_records(query)
        exists = boolean(exists_record[0])

        if not exists:
            missing_experiments.append(experiment_id)

        # To not DDOS redshift
        time.sleep(0.1)

    return missing_experiments


def check_queries_for_table_name(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    records = task_instance.xcom_pull(
        task_ids='gather_manually_mapped_tables')
    found_records = set()

    continuous_path_base_dir = 'manual_populations/continuous_update'
    onetime_path_base_dir = 'manual_populations/run_once'

    continuous_paths = [os.path.join(continuous_path_base_dir, f) for f in os.listdir(
        continuous_path_base_dir) if os.path.isfile(os.path.join(continuous_path_base_dir, f))]
    onetime_paths = [os.path.join(onetime_path_base_dir, f) for f in os.listdir(
        onetime_path_base_dir) if os.path.isfile(os.path.join(onetime_path_base_dir, f))]

    filepaths = continuous_paths + onetime_paths
    for fp in filepaths:
        with open(fp, 'r') as f:
            s = f.read()

            for experiment_id, table_name in records.items():
                if experiment_id in found_records:
                    continue
                index = s.find(table_name)
                if index != -1:
                    found_records.add(experiment_id)
                    continue
    return found_records


def identify_missing_tables(conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    tables_by_experiments = task_instance.xcom_pull(
        task_ids='gather_manually_mapped_tables')
    missing_experiments_from_db = task_instance.xcom_pull(
        task_ids='check_table_existence'
    )
    missing_experiments_from_files = task_instance.xcom_pull(
        task_ids='check_queries_for_table_name'
    )

    all_experiments = set(tables_by_experiments.keys())
    missing_experiments = all_experiments.difference(
        set(missing_experiments_from_db)).difference(set(missing_experiments_from_files))

    for experiment in missing_experiments:
        print("Missing table for experiment_id %s: %s" %
              (experiment, tables_by_experiments[experiment]))

    if missing_experiments:
        conf = kwargs['conf']
        if conf.getboolean('scheduler', 'statsd_on'):
            client = statsd.StatsClient(
                host=conf.get('scheduler', 'statsd_host'),
                port=conf.get('scheduler', 'statsd_port'),
                prefix=conf.get('scheduler', 'statsd_prefix'),
            )
            client.incr('validation_dag.missing_tables',
                        len(missing_experiments))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('experimental_population_validation',
         start_date=datetime(2020, 11, 9, 17),  # Starts at 5pm PST
         max_active_runs=1,
         catchup=False,
         schedule_interval=timedelta(minutes=10),
         default_args=default_args,
         ) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    gather_manually_mapped_tables_task = PythonOperator(
        task_id='gather_manually_mapped_tables',
        python_callable=gather_manually_mapped_tables,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True,
    )

    check_table_existence_task = PythonOperator(
        task_id='check_table_existence',
        python_callable=check_table_existence,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True,
    )

    check_queries_for_table_name_task = PythonOperator(
        task_id='check_queries_for_table_name',
        python_callable=check_queries_for_table_name,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True,
    )

    identify_missing_tables_task = PythonOperator(
        task_id='identify_missing_tables',
        python_callable=identify_missing_tables,
        op_kwargs={'conn_id'}
    )

    end_task = DummyOperator(task_id='end')

    start_task >> gather_manually_mapped_tables_task >> [
        check_table_existence_task, check_queries_for_table_name_task] >> identify_missing_tables_task >> end_task
