'''
Sync DEV Data
'''


from airflow import DAG
from datetime import datetime, timedelta
import os
from templates import DIR as template_dir
from calm_logger.logging_helper import setup_logging


logger = setup_logging(__name__)

TEMPLATE_SEARCHPATH = f'{template_dir}/sql'
POSTGRESS_CONN_ID = 'redshift'
REDSHIFT_CONN_ID = 'redshift'
REDSHIFT_DB = 'data_dev'

# sqlalchemy connection is different from normal pg because psycopg scheme
GE_SQL_CONN = os.environ.get('AIRFLOW_CONN_SQLALCHEMY_REDSHIFT')

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 21),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

sync_dev_data = DAG(
    'sync_dev_data',
    default_args=default_args,
    schedule_interval='34 0 * * *',
    template_searchpath=TEMPLATE_SEARCHPATH,
    max_active_runs=1)


# schemas and tables to sync
# ----------------------------
# appdb.*
# appsflyer
# data.id_map
# data.idfa_map
# enrichment.country_info
# fb.ad_insights
# financial.country_proceed_and_tax_rates
# financial.currency_exchange_rates
# financial.itunes_prod_price_by_cc_by_month
# financial.itunes_prod_recent_price_by_cc
# financial.plan_price_estimates
# sendgrid.*
# stripe.*
# helper.*
# zendesk.*


# task flow
# -------------

# for each table
# ------------------
# query to generate create table statement for each
# pass query to next task with xcom
# create a staging table for the table we want to swap
# unload data for table
# copy data into staging table
# rename table to old table
# rename staging table to table
# simple parity test between tables
# drop old table
