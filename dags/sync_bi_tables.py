'''
refreshes bi_tables:
daily_calm_titles', 'guide_variants_info', 'ab_test_enrollments_flat', 'device_acnts', 'ios_subscriptions_w_acnts'
ios_is_nonreturning_autorenewal_subs',  'sessions_all' 'subscriptions' 'accounts', 'appsflyer_conversions'
'integrated_ratings'

Schedule: every hour at minute 34
Tasks:
1. Create staging tables in layers based on dependencies
2. Run great expectations tests on the new tables
3. replace tables with the staging table
'''


from airflow import DAG
from datetime import datetime, timedelta
import os
from airflow.operators import PostgresOperator
from templates import DIR as template_dir
from operators.great_expectations_operator import GreatExpectationsSqlContextOperator
from calm_logger.logging_helper import setup_logging


logger = setup_logging(__name__)

TEMPLATE_SEARCHPATH = f'{template_dir}/sql'
FIRST_TABLES = ['daily_calm_titles', 'guide_variants_info', 'ab_test_enrollments_flat',
                'device_acnts', 'ios_subscriptions_w_acnts']
SECOND_TABLES = ['ios_is_nonreturning_autorenewal_subs',  'sessions_all']
THIRD_TABLES = ['subscriptions']
FOURTH_TABLES = ['accounts', 'appsflyer_conversions']
FIFTH_TABLES = ['integrated_ratings']
EXPECTATION_TESTS = ['device_acnts_cc_lower', 'subscriptions_prices_not_null', 'subscriptions_proceeds_not_null',
                     'subscriptions_prod_price_paid', 'subscriptions_prod_price_free',
                     'subscriptions_paid_trans_more_than_zero', 'subscriptions_n_more_than_one',
                     'subscriptions_n_paid_more_than_zero', 'subscriptions_refunded_more_than_zero']

REDSHIFT_CONN_ID = 'redshift'
REDSHIFT_DB = 'stitch'
STAGING_SCHEMA = 'dev'
BI_SCHEMA = 'bi'
REPLACE_SQL = 'replace_table_with_stage.sql'

# sqlalchemy connection is different from normal pg because psycopg scheme
GE_SQL_CONN = os.environ.get('AIRFLOW_CONN_SQLALCHEMY_REDSHIFT')

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 23),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

bi_tables_dag = DAG(
    'sync_bi_tables',
    default_args=default_args,
    schedule_interval='34 * * * *',
    template_searchpath=TEMPLATE_SEARCHPATH)


# returns an instance of the PostgresOperator
def create_postgres_operator(table_name, task_id, sql, params):
    return PostgresOperator(
        dag=bi_tables_dag,
        task_id=task_id,
        postgres_conn_id='redshift',
        sql=sql,
        db=REDSHIFT_DB,
        params=params
    )


# because of the layered nature of creating tables in bi_tables_sync,
# a function that adds the tasks for each layer to an arry and sets upstream
def create_staging_task_groups(tables_array, previous_tables_array=None):
    task_array = []
    for table_name in tables_array:
        task_id = f'create_staging_table_{table_name}'
        sql = f'sync_bi_tables/{table_name}.sql'
        params = {'schema': STAGING_SCHEMA}
        task = create_postgres_operator(table_name, task_id, sql, params)
        if previous_tables_array:
            task.set_upstream(previous_tables_array)
        task_array.append(task)

    return task_array


# create tasks for each layer of tables
first_stage_table_tasks = create_staging_task_groups(FIRST_TABLES)

second_stage_table_tasks = create_staging_task_groups(SECOND_TABLES, first_stage_table_tasks)

third_stage_table_tasks = create_staging_task_groups(THIRD_TABLES, second_stage_table_tasks)

fourth_stage_table_tasks = create_staging_task_groups(FOURTH_TABLES, third_stage_table_tasks)

fifth_stage_table_tasks = create_staging_task_groups(FIFTH_TABLES, fourth_stage_table_tasks)

# run all the expectations tests
run_expectation_tasks = []
for test in EXPECTATION_TESTS:
    run_expectation = GreatExpectationsSqlContextOperator(
        task_id=f'run_expectation_{test}',
        sql_conn=GE_SQL_CONN,
        data_set=test,
        sql=f'sync_bi_tables/ge_dataset_sql/{test}.sql',
        validation_config=f'{test}.json',
        dag=bi_tables_dag,
        params={'schema': STAGING_SCHEMA}
    )
    run_expectation_tasks.append(run_expectation)
    run_expectation.set_upstream(fifth_stage_table_tasks)

# rename tables to the non-staging version
for table in FIRST_TABLES + SECOND_TABLES + THIRD_TABLES + FOURTH_TABLES + FIFTH_TABLES:
    params = {
        'schema': STAGING_SCHEMA,
        'table_name': table,
        'stage_name': f'{table}_stage'
    }
    task = create_postgres_operator(table, f'replace_{table}_with_stage', REPLACE_SQL, params)
    task.set_upstream(run_expectation_tasks)
