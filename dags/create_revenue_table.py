from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta


PURCHASE_EVENT_TABLE = 'temp.purchase_events'


def success_callback(ctx):
    pass


def failure_callback(ctx):
    pass


def drop_create_revenue_table(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)
    query = '''
    begin;
    DROP TABLE IF EXISTS {table};
    CREATE TABLE IF NOT EXISTS {table} (
        event_date timestamp encode AZ64,
        servicename varchar(256) encode ZSTD,
        entity_id varchar(32) encode ZSTD,
        event_name varchar(64) encode ZSTD,
        original_transaction_id varchar(256) encode ZSTD,
        transaction_id varchar(256) encode ZSTD DISTKEY,
        product_id varchar(256) encode ZSTD,
        product_name varchar(256) encode ZSTD
    )
    COMPOUND SORTKEY(event_date, event_name);
    GRANT ALL ON TABLE {table} TO GROUP team;
    commit;
    '''.format(**{'table': PURCHASE_EVENT_TABLE})
    pg_hook.run(query)


def get_ios_free_trial_events(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    INSERT INTO {table}
    SELECT
        iap.purchasedate,
        p.serviceName,
        p.entityid,
        'started_free_trial',
        p.servicetransactionid,
        iap.transactionid,
        iap.productid,
        p.productname
    FROM
        frog.purchases p
    JOIN
        production.ios_iap_receipt iap on p.servicetransactionid = iap.originaltransactionid
    WHERE
        iap.istrialperiod = true;
    '''.format(**{'table': PURCHASE_EVENT_TABLE})
    pg_hook.run(query)


def get_ios_refund_events(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    INSERT INTO {table}
    SELECT
        iap.purchasedate,
        p.serviceName,
        p.entityid,
        'refund_granted',
        p.servicetransactionid,
        iap.transactionid,
        iap.productid,
        p.productname
    FROM
        frog.purchases p
    JOIN
        production.ios_iap_receipt iap on p.servicetransactionid = iap.originaltransactionid
    WHERE
        iap.cancellationdate is not null;
    '''.format(**{'table': PURCHASE_EVENT_TABLE})
    pg_hook.run(query)


def get_ios_expired_events(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    INSERT INTO {table}
    SELECT
        iap.expiresdate,
        p.serviceName,
        p.entityid,
        'paid_transaction_expired',
        p.servicetransactionid,
        iap.transactionid,
        iap.productid,
        p.productname
    FROM
        frog.purchases p
    JOIN
        production.ios_iap_receipt iap on p.servicetransactionid = iap.originaltransactionid
    WHERE
        iap.istrialperiod = false and
        iap.cancellationdate is null and
        iap.expirationintent is not null
    '''.format(**{'table': PURCHASE_EVENT_TABLE})
    pg_hook.run(query)


def get_ios_payment_failed_events(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    INSERT INTO {table}
    SELECT
        iap.purchasedate,
        p.serviceName,
        p.entityid,
        'payment_failed',
        p.servicetransactionid,
        iap.transactionid,
        iap.productid,
        p.productname
    FROM
        frog.purchases p
    JOIN
        production.ios_iap_receipt iap on p.servicetransactionid = iap.originaltransactionid
    WHERE
        iap.istrialperiod = false and
        iap.cancellationdate is null and
        iap.expirationintent = 2
    '''.format(**{'table': PURCHASE_EVENT_TABLE})
    pg_hook.run(query)


def get_ios_payment_events(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    INSERT INTO {table}
    SELECT
        iap.purchasedate,
        p.serviceName,
        p.entityid,
        'paid_transaction',
        p.servicetransactionid,
        iap.transactionid,
        iap.productid,
        p.productname
    FROM
        frog.purchases p
    JOIN
        production.ios_iap_receipt iap on p.servicetransactionid = iap.originaltransactionid
    WHERE
        iap.istrialperiod = false and
        (iap.expirationintent != 2 or iap.expirationintent is null)
    '''.format(**{'table': PURCHASE_EVENT_TABLE})
    pg_hook.run(query)


def get_ios_unknown_events(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    INSERT INTO {table}
    SELECT
        iap.purchasedate,
        p.serviceName,
        p.entityid,
        'unknown_error',
        p.servicetransactionid,
        iap.transactionid,
        iap.productid,
        p.productname
    FROM
        frog.purchases p
    JOIN
        production.ios_iap_receipt iap on p.servicetransactionid = iap.originaltransactionid
    WHERE
        iap.istrialperiod = false and
        iap.cancellationdate is null and
        iap.expirationintent = 5
    '''.format(**{'table': PURCHASE_EVENT_TABLE})
    pg_hook.run(query)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG('create_revenue_table',
         start_date=datetime(2020, 9, 30, 17),
         max_active_runs=1,
         catchup=False,
         schedule_interval='@daily',
         default_args=default_args,
         on_success_callback=success_callback,
         on_failure_callback=failure_callback
         ) as dag:

    # Tasks for population
    start_task = DummyOperator(
        task_id='start'
    )

    drop_create_revenue_table_task = PythonOperator(
        task_id='drop_create_revenue_table',
        python_callable=drop_create_revenue_table,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    get_ios_free_trial_events_task = PythonOperator(
        task_id='get_ios_free_trial_events',
        python_callable=get_ios_free_trial_events,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )
    get_ios_refund_events_task = PythonOperator(
        task_id='get_ios_refund_events',
        python_callable=get_ios_refund_events,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )
    get_ios_expired_events_task = PythonOperator(
        task_id='get_ios_expired_events',
        python_callable=get_ios_expired_events,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )
    get_ios_payment_failed_events_task = PythonOperator(
        task_id='get_ios_payment_failed_events',
        python_callable=get_ios_payment_failed_events,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )
    get_ios_payment_events_task = PythonOperator(
        task_id='get_ios_payment_events',
        python_callable=get_ios_payment_events,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )
    get_ios_unknown_events_task = PythonOperator(
        task_id='get_ios_unknown_events',
        python_callable=get_ios_unknown_events,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )

    finish_ios_task = DummyOperator(
        task_id='finish_ios'
    )

    start_task >> drop_create_revenue_table_task >> [
        get_ios_free_trial_events_task, get_ios_refund_events_task,
        get_ios_expired_events_task, get_ios_payment_failed_events_task,
        get_ios_payment_events_task, get_ios_unknown_events_task] >> finish_ios_task
