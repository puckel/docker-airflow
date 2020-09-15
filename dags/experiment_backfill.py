from airflow import DAG

from airflow.hooks import PostgresHook
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

from experimental_platform_modules import result_calculator

import pandas
import uuid
import time

CONTROL_PANEL_TABLE_ONLY = 'experiment_control_panel'
EXPERIMENTAL_METADATA_TABLE = 'ab_platform.backfill_run'


def _create_run_metadata_table(conn_id):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS {} (
        run_id VARCHAR(36) ENCODE ZSTD DISTKEY,
        status VARCHAR(128) ENCODE ZSTD,
        createdat TIMESTAMP DEFAULT sysdate
    )
    COMPOUND SORTKEY(end_ts, start_ts)
    ;
    '''.format(EXPERIMENTAL_METADATA_TABLE,)
    pg_hook.run(query)


def _callback(state, ctx):
    conn_id = 'analytics_redshift'
    pg_hook = PostgresHook(conn_id)
    run_uuid = uuid.uuid4()
    _create_run_metadata_table(conn_id)

    query = '''
    INSERT INTO {} (run_id, status)
    VALUES('{}', '{}')
    '''.format(EXPERIMENTAL_METADATA_TABLE, run_uuid, state)
    pg_hook.run(query)


def success_callback(ctx):
    _callback('success', ctx)


def failure_callback(ctx):
    _callback('failure', ctx)


def get_backfill_dates(analytics_conn_id, frontend_conn_id, ts, **kwargs):
    analytics_pg_hook = PostgresHook(analytics_conn_id)
    frontend_pg_hook = PostgresHook(frontend_conn_id)

    missing_days = []
    query = '''
    SELECT * from ab_platform.{} WHERE archived = false
    '''.format(CONTROL_PANEL_TABLE_ONLY)
    experiments = pandas.read_sql_query(
        query, analytics_pg_hook.get_sqlalchemy_engine(),
        parse_dates=['start_date'])

    dates_and_metrics_query = '''
    SELECT
        experiment_id,
        metric_name,
        day
    FROM
        ab_platform.experiment_results_intermediate
    GROUP BY 1,2,3
    '''

    experiment_results_by_day = pandas.read_sql_query(
        dates_and_metrics_query, frontend_pg_hook.get_sqlalchemy_engine(),
        parse_dates=['day']
    )

    metric_names_by_experiment_id = dict([
        (
            experiment['experiment_id'],
            result_calculator.get_metric_names(experiment['population_kind'])
        )
        for _, experiment in experiments.iterrows()
    ])

    start_date_by_experiment_id = dict([
        (experiment['experiment_id'], experiment['start_date'])
        for _, experiment in experiments.iterrows()
    ])

    # Currently if we're missing any days for any experiment, we just rerun that entire day
    for experiment_id, metric_names in metric_names_by_experiment_id.items():
        start_date = start_date_by_experiment_id[experiment_id]

        temp_date = start_date
        metric_by_day = {}
        for _, result in experiment_results_by_day.iterrows():
            if result['experiment_id'] != experiment_id:
                continue

            # Check if we're missing a chunk of days
            day = result['day']
            # This will give us the number of days difference between the two dates
            delta = (day - temp_date).days
            if delta > 0:
                extra_days = [temp_date +
                              timedelta(days=i) for i in range(delta)]
                missing_days += extra_days

            # Check if we're missing any metrics on this day
            metric_set = metric_by_day.get(day, set())
            metric_set.add(result['metric_name'])
            metric_by_day[day] = metric_set
            temp_date = day

        for day, metric_set in metric_by_day.items():
            # If we're missing any metrics add the day to missing days
            if set(metric_names) - metric_set:
                missing_days.append(day)

    # Return the missing_day_set
    # After deduping
    missing_day_set = set(missing_days)
    print("Found %s missing days" % len(missing_day_set))
    return missing_day_set


def backfill_intermediate_results(analytics_conn_id, frontend_conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']

    backfill_dates = task_instance.xcom_pull(
        task_ids='get_backfill_dates'
    )

    # Do it all in one go instead of piecemeal like the result calculator graph
    for dt in backfill_dates:
        try:
            # We have to get the list of active experiments on each particular date to be able to accurately
            # Run the backfill
            print("Running backfill for {}".format(dt.isoformat()))
            experiment_to_population_map = result_calculator.get_active_experiment_and_population_map(
                analytics_conn_id, dt)
            print("Found active {} active experiments {}".format(
                len(experiment_to_population_map), experiment_to_population_map.keys()))

            records = result_calculator.calculate_intermediate_result_for_day(
                analytics_conn_id, dt,
                experiment_to_population_map
            )
            result_calculator.insert_intermediate_records(
                frontend_conn_id, records)
            time.sleep(1.0)
        except Exception as e:
            print("Got error {}".format(e))
            print ("Continuing to next date")
            continue


def calculate_results(frontend_conn_id, **kwargs):
    result_calculator.calculate_results(frontend_conn_id)


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

default_task_kwargs = {
    'analytics_conn_id': 'analytics_redshift',
    'frontend_conn_id': 'ab_platform_frontend'
}

with DAG('experimental_backfill',
         start_date=datetime(2020, 6, 10),
         max_active_runs=1,
         catchup=False,
         schedule_interval=None,
         default_args=default_args,
         on_success_callback=success_callback,
         on_failure_callback=failure_callback
         ) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    trigger_control_panel_sync_task = TriggerDagRunOperator(
        task_id='trigger_control_panel_sync',
        trigger_dag_id='experimental_control_panel_ingest'
    )

    # We'll eventually split this dag apart
    # This will trigger the population creation dag
    trigger_event_ingest_task = TriggerDagRunOperator(
        task_id='trigger_event_ingest',
        trigger_dag_id='experimental_event_ingest'
    )

    get_backfill_dates_task = PythonOperator(
        task_id='get_backfill_dates',
        python_callable=get_backfill_dates,
        op_kwargs=default_task_kwargs,
        provide_context=True,
    )

    backfill_intermediate_results_task = PythonOperator(
        task_id='backfill_intermediate_results',
        python_callable=backfill_intermediate_results,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )
    # backfill_intermediate_results_task = DummyOperator(
    #     task_id='backfill_intermediate_results')

    calculate_results_task = PythonOperator(
        task_id='calculate_results',
        python_callable=calculate_results,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )
    # calculate_results_task = DummyOperator(task_id='calculate_results')

    start_task >> trigger_control_panel_sync_task >> \
        trigger_event_ingest_task >> \
        get_backfill_dates_task >> \
        backfill_intermediate_results_task >> calculate_results_task
