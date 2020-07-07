from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from dateutil import parser
from scipy.stats import ttest_ind_from_stats
from statsmodels.stats.proportion import proportions_chisquare

import os
import pathlib
import pandas
import sqlalchemy
import uuid

CONTROL_PANEL_TABLE = 'ab_platform.experiment_control_panel'
POPULATION_MAPPING_TABLE = 'ab_platform.experiment_to_population_map'
EXPERIMENT_INTERMEDIATE_RESULTS_TABLE = 'ab_platform.experiment_results_intermediate'
EXPERIMENT_INTERMEDIATE_RESULTS_TABLE_ONLY = 'experiment_results_intermediate'
RESULTS_METADATA_TABLE = 'ab_platform.results_run'


def _create_results_run_table(conn_id):
    pg_hook = PostgresHook(conn_id)
    query = '''
    CREATE TABLE IF NOT EXISTS {} (
        run_id VARCHAR(36) ENCODE ZSTD distkey,
        status VARCHAR(128) ENCODE ZSTD,
        intermediate_results_date TIMESTAMP,
        createdat TIMESTAMP DEFAULT sysdate
    )
    COMPOUND SORTKEY(createdat)
    ;
    '''.format(RESULTS_METADATA_TABLE)
    pg_hook.run(query)


def _callback(state, ctx):
    task_instance = ctx['task_instance']
    conn_id = 'analytics_redshift'
    pg_hook = PostgresHook(conn_id)
    intermediate_results_run_date = task_instance.xcom_pull(
        key='intermediate_results_run_date'
    )
    run_id = uuid.uuid4()

    _create_results_run_table(conn_id)

    query = '''
    INSERT INTO {} (run_id, status, intermediate_results_date) VALUES
    ('{}', '{}', '{}'::TIMESTAMP)
    '''.format(RESULTS_METADATA_TABLE, run_id, state, intermediate_results_run_date.isoformat())
    pg_hook.run(query)


def success_callback(ctx):
    _callback('success', ctx)


def failure_callback(ctx):
    _callback('failure', ctx)


def get_active_experiment_and_population_map(analytics_conn_id, ts, **kwargs):
    pg_hook = PostgresHook(analytics_conn_id)
    query = '''
    SELECT
        a.experiment_id,
        a.population_kind,
        b.table_name
    FROM %s a JOIN %s b ON a.experiment_id = b.experiment_id
    WHERE getdate() > start_date and archived = false;
    ''' % (CONTROL_PANEL_TABLE, POPULATION_MAPPING_TABLE)
    records = pg_hook.get_records(query)
    experiment_id_to_table_mapping = {}
    for experiment_id, population_kind, table_name in records:
        experiment_id_to_table_mapping[experiment_id] = {
            'population_table_name': table_name,
            'population_schema_name':  'ab_platform',
            'population_type': population_kind,
        }
    return experiment_id_to_table_mapping


def create_intermediate_results_table(frontend_conn_id, ts, **kwargs):
    pg_hook = PostgresHook(frontend_conn_id)
    query = '''
    create table if not exists %s
    (
        experiment_id varchar(36) not null,
        variant varchar(128) not null,
        metric_name varchar(128) not null,
        metric_type varchar(128) not null,
        segment varchar(128) not null,
        day timestamp not null,
        numerator integer,
        denominator integer,
        mean double precision,
        standard_deviation double precision,
        primary key(experiment_id, variant, metric_name, metric_type, segment, day)
    );
    ''' % EXPERIMENT_INTERMEDIATE_RESULTS_TABLE
    pg_hook.run(query)


def create_final_results_table(frontend_conn_id, ts, **kwargs):
    pass


def calculate_intermediate_results(analytics_conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(analytics_conn_id)
    dt = parser.parse(ts)
    # Get the last days worth of stuff
    # Use this instead of the provided 'ds' so we can do some date operations
    yesterday = dt.date() - timedelta(days=1)
    task_instance.xcom_push(
        key='intermediate_results_run_date', value=yesterday)

    experiment_to_population_map = task_instance.xcom_pull(
        task_ids='get_active_experiment_and_population_map'
    )

    population_types = set([population_metadata['population_type']
                            for population_metadata in experiment_to_population_map.values()])

    population_templates = {}
    for population_type in population_types:
        population_templates[population_type] = {}
        parent_dir = os.path.join('./queries', population_type.lower())
        for item in os.listdir(parent_dir):
            item_path = os.path.join(parent_dir, item)
            metric_name = item.split('.')[0]
            with open(item_path, 'r') as f:
                s = f.read()
                population_templates[population_type][metric_name] = s

    all_records = []
    for experiment_id, population_metadata in experiment_to_population_map.items():
        print('Getting the intermediate results for {}'.format(experiment_id))
        template_map = population_templates.get(
            population_metadata['population_type'], {})
        for metric_name, template in template_map.items():
            print(metric_name)
            s = template % {
                'metric_name': metric_name,
                'table_name': '%s.%s' % (population_metadata['population_schema_name'],
                                         population_metadata['population_table_name']),
                'ds': yesterday.isoformat(),
            }
            records = pg_hook.get_records(s)
            all_records += records

    return all_records


def insert_intermediate_records(frontend_conn_id, ts, **kwargs):
    task_instance = kwargs['task_instance']
    pg_hook = PostgresHook(frontend_conn_id)
    records = task_instance.xcom_pull(
        task_ids='calculate_intermediate_results'
    )
    # We have to worry about double inserts if it runs on the same day
    # So we delete records from the table with the same experiment_id, metric_name, and date

    for experiment_id, variant, metric_name, metric_type, \
            segment, day, numerator, denominator, mean, \
            standard_deviation in records:

        query = '''
            begin;
            DELETE FROM %(table_name)s
            WHERE
                experiment_id = '%(experiment_id)s' and
                variant = '%(variant)s' and
                metric_name = '%(metric_name)s' and
                metric_type = '%(metric_type)s' and
                segment = '%(segment)s' and
                day = '%(day)s';

            INSERT INTO %(table_name)s
            (experiment_id, variant, metric_name, metric_type, segment, day, numerator, denominator, mean, standard_deviation) VALUES
            ('%(experiment_id)s', '%(variant)s', '%(metric_name)s', '%(metric_type)s', '%(segment)s', '%(day)s', %(numerator)d, %(denominator)d, %(mean)f, %(standard_deviation)f);
            commit;
            ''' % {
            'table_name': EXPERIMENT_INTERMEDIATE_RESULTS_TABLE,
            'experiment_id': experiment_id,
            'variant':  variant,
            'metric_name': metric_name,
            'metric_type': metric_type,
            'segment': segment,
            'day': day.date().isoformat(),
            'numerator': numerator,
            'denominator': denominator,
            'mean': mean,
            'standard_deviation': standard_deviation,
        }
        pg_hook.run(query)


def _calculate_p_value(x):
    p_value = None
    if x['variant'] == x['variant_compared']:
        pass

    elif x['metric_type'].lower() == 'ratio':
        result = ttest_ind_from_stats(
            mean1=x['mean'],
            std1=x['standard_deviation'],
            nobs1=x['denominator'],
            mean2=x['mean_compared'],
            std2=x['standard_deviation_compared'],
            nobs2=x['denominator_compared']
        )
        p_value = result.pvalue
    elif x['metric_type'].lower() == 'proportional':
        numerators = (x['numerator'], x['numerator_compared'])
        denominators = (x['denominator'], x['denominator_compared'])
        result = proportions_chisquare(numerators, denominators)
        p_value = result[1]

    return p_value


def calculate_results(frontend_conn_id, ts, **kwargs):
    # Take the whole table and replace
    # Naturally idempotent, we can use the pandas helper methods
    pg_hook = PostgresHook(frontend_conn_id)

    results_intermediate = pandas.read_sql_table(EXPERIMENT_INTERMEDIATE_RESULTS_TABLE_ONLY,
                                                 pg_hook.get_sqlalchemy_engine(), schema='ab_platform')

    print("Found {} intermediate results in intermediate table".format(
        len(results_intermediate)))
    results_intermediate.set_index(
        ['experiment_id', 'metric_name', 'metric_type', 'segment', 'day'], inplace=True)

    results_expanded = results_intermediate.join(
        results_intermediate, rsuffix='_compared').reset_index()
    results_expanded['p_value'] = results_expanded.apply(
        _calculate_p_value, axis=1)

    print("Writing {} results to RDS".format(len(results_expanded)))

    results_expanded.to_sql(
        name='experiment_results',
        con=pg_hook.get_sqlalchemy_engine(),
        schema='ab_platform',
        if_exists='replace',
        index=False,
        dtype={
            "experiment_id": sqlalchemy.types.VARCHAR(36),
            "variant": sqlalchemy.types.VARCHAR(128),
            "metric_name": sqlalchemy.types.VARCHAR(128),
            "metric_type": sqlalchemy.types.VARCHAR(128),
            "segment": sqlalchemy.types.VARCHAR(128),
            "day": sqlalchemy.types.DateTime,
            "numerator": sqlalchemy.types.Integer,
            "denominator": sqlalchemy.types.Integer,
            "mean": sqlalchemy.types.Float,
            "standard_deviation": sqlalchemy.types.Float,
            "p_value": sqlalchemy.types.Float,
            "variant_compared": sqlalchemy.types.VARCHAR(128),
            "numerator_compared": sqlalchemy.types.Integer,
            "denominator_compared": sqlalchemy.types.Integer,
            "mean_compared": sqlalchemy.types.Float,
            "standard_deviation_compared": sqlalchemy.types.Float,
        }
    )
    print("Done writing results to RDS")


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG('experimental_results_calculator',
         start_date=datetime(2020, 6, 25, 17),  # Starts at 5pm PST
         max_active_runs=1,
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args,
         on_failure_callback=failure_callback,
         on_success_callback=success_callback,
         ) as dag:

    default_task_kwargs = {
        'analytics_conn_id': 'analytics_redshift',
        'frontend_conn_id': 'ab_platform_frontend',
    }

    start_task = DummyOperator(
        task_id='start'
    )

    create_intermediate_results_table_task = PythonOperator(
        task_id='create_intermediate_results_table',
        python_callable=create_intermediate_results_table,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    get_active_experiment_and_population_map_task = PythonOperator(
        task_id='get_active_experiment_and_population_map',
        python_callable=get_active_experiment_and_population_map,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    calculate_intermediate_results_task = PythonOperator(
        task_id='calculate_intermediate_results',
        python_callable=calculate_intermediate_results,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    insert_intermediate_records_task = PythonOperator(
        task_id='insert_intermediate_results',
        python_callable=insert_intermediate_records,
        op_kwargs=default_task_kwargs,
        provide_context=True
    )

    calculate_results_task = PythonOperator(
        task_id='caculate_results',
        python_callable=calculate_results,
        op_kwargs=default_task_kwargs,
        provide_context=True,
    )

    start_task >> [get_active_experiment_and_population_map_task,
                   create_intermediate_results_table_task]

    [get_active_experiment_and_population_map_task, create_intermediate_results_table_task] >> \
        calculate_intermediate_results_task >> insert_intermediate_records_task >> \
        calculate_results_task
