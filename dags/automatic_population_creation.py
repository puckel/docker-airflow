import os
import statsd
import time
import uuid

from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from experimental_platform_modules import util

from datetime import datetime, timedelta

RUN_TRACKER_TABLE = 'ab_platform.automatic_population_creation_run'
POPULATION_MAPPING_TABLE = 'ab_platform.experiment_to_population_map'

def _create_run_tracker_table (conn_id):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS {} (
        run_id VARCHAR(36) ENCODE ZSTD DISTKEY,
        status VARCHAR(128) ENCODE BYTEDICT,
        createdat TIMESTAMP DEFAULT SYSDATE
    )
    COMPOUND SORTKEY(createdat);
    '''.format(POPULATION_METADATA_TABLE)
    pg_hook.run(query)


def _mark_run_state (state, ctx):
    conn_id = 'analytics_redshift'
    pg_hook = PostgresHook(conn_id)
    run_uuid = uuid.uuid4()

    # Create metadata table if it doesn't exist
    _create_run_tracker_table(conn_id)

    query = '''
    INSERT INTO {} (run_id, status) VALUES ('{}', '{}')
    '''.format(RUN_TRACKER_TABLE, run_uuid, state)
    pg_hook.run(query)

def success_callback(ctx):
    _mark_run_state('success', ctx)


def failure_callback(ctx):
    _mark_run_state('failure', ctx)

def get_experiment_names (conn_id):
    pg_hook = PostgresHook(conn_id)
    query = 'SELECT distinct experiment FROM logs.experiment_start'
    return pg_hook.get_records(query)

def get_create_population_query (experiment_name):
    return '''
    DROP TABLE IF EXISTS ab_platform.experiment_%(experiment_name)s_updated;
    CREATE TABLE ab_platform.experiment_%(experiment_name)s_updated distkey(entity_id) compound sortkey(variant, day, entityType)
    AS
    SELECT
      experiment as experiment_id,
      variant,
      entityid as entity_id,
      entityType as entity_type,
      min(day) as entered_at
    FROM logs.experiment_start
    WHERE experiment = '%(experiment_name)s'
    GROUP BY
      entity_id,
      variant,
      experiment_id,
      entity_type
    BEGIN;
    DROP TABLE IF EXISTS ab_platform.experiment_%(experiment_name)s;
    ALTER ab_platform.experiment_%(experiment_name)s_updated RENAME TO experiment_%(experiment_name)s;
    GRANT ALL ON ab_platform.experiment_%(experiment_name)s TO GROUP team;
    COMMIT;
    ''' % {
      'experiment_name': experiment_name,
    }

def create_population_table (conn_id, experiment_name):
    pg_hook = PostgresHook(conn_id)
    query = get_create_population_query(experiment_name)
    return pg_hook.run(query)


def create_automatic_populations (conn_id, ts, **kwargs):
    experiment_names = get_experiment_names(conn_id)
    for experiment_name in experiment_names:
      create_population_table(conn_id, experiment_name)
      insert_experiment_mapping_row(conn_id, experiment_name)

def get_insert_mapping_row_query (experiment_name):
  return '''
  begin;
  DELETE FROM %(mapping_table)s
  WHERE experiment_id='%(experiment_id)s';
  INSERT INTO %(mapping_table)s (experiment_id, table_name)
  VALUES ('%(experiment_id)s', '%(table_name)s');
  commit;
  ''' % {
      'mapping_table': POPULATION_MAPPING_TABLE,
      'experiment_id': experiment_name,
      'table_name': 'experiment_' + experiment_name,
  }

def insert_experiment_mapping_row (conn_id, experiment_name):
    pg_hook = PostgresHook(conn_id)
    query = get_insert_mapping_row_query(experiment_name)
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



with DAG('create_automatic_populations',
         start_date=datetime(2020, 6, 25, 17),  # Starts at 5pm PST
         max_active_runs=1,
         catchup=False,
         schedule_interval='@daily',
         default_args=default_args,
         on_success_callback=success_callback,
         on_failure_callback=failure_callback,
         ) as dag:

    # Tasks for population
    start_task = DummyOperator(
        task_id='start'
    )

    create_automatic_populations_task = PythonOperator(
        task_id='create_automatic_populations',
        python_callable=create_automatic_populations,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=False
    )

    end_task = DummyOperator(task_id='end')
    start_task >> create_automatic_populations_task >> insert_experiment_mapping_rows >> end_task
