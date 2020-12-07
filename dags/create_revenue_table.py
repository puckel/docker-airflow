from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta


PURCHASE_EVENT_TABLE = 'logs.purchase_events'


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
        product_name varchar(256) encode ZSTD,
        expires_date timestamp encode AZ64
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
        p.productname,
        iap.expiresdate
    FROM
        frog.purchases p
    JOIN
        production.ios_iap_receipt iap on p.servicetransactionid = iap.originaltransactionid
    WHERE
        iap.istrialperiod = true;
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
        p.productname,
        null
    FROM
        frog.purchases p
    JOIN
        production.ios_iap_receipt iap on p.servicetransactionid = iap.originaltransactionid
    JOIN
        production.ios_subscription_notification sub on iap.transactionid = sub.transactionid
    WHERE
        iap.istrialperiod = false and
        iap.cancellationdate is null and
        sub.notificationtype = 'DID_FAIL_TO_RENEW'
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
        p.productname,
        iap.expiresdate
    FROM
        frog.purchases p
    JOIN
        production.ios_iap_receipt iap on p.servicetransactionid = iap.originaltransactionid
    WHERE
        iap.istrialperiod = false
    '''.format(**{'table': PURCHASE_EVENT_TABLE})
    pg_hook.run(query)


def get_ios_cancellation_events(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    INSERT INTO {table}
    SELECT
        iap.cancellationdate,
        p.serviceName,
        p.entityid,
        'cancel',
        p.servicetransactionid,
        iap.transactionid,
        iap.productid,
        p.productname,
        null
    FROM
        frog.purchases p
    JOIN
        production.ios_iap_receipt iap on p.servicetransactionid = iap.originaltransactionid
    WHERE
        iap.istrialperiod = false and
        iap.cancellationdate is not null
    '''.format(**{'table': PURCHASE_EVENT_TABLE})
    pg_hook.run(query)


def get_android_free_trial_events(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    INSERT INTO {table}
    SELECT
        snap.starttimemillis,
        p.serviceName,
        p.entityid,
        'started_free_trial',
        p.servicetransactionid,
        snap.orderid,
        snap.productid,
        p.productname,
        snap.expirytimemillis
    FROM
        frog.purchases p
    JOIN
        production.android_subscription_snapshot snap on p.servicetransactionid = snap.purchasetoken
    WHERE
        snap.paymentstate = 3;

    '''.format(**{'table': PURCHASE_EVENT_TABLE})
    pg_hook.run(query)


def get_android_payment_events(conn_id, ts, **kwargs):
    pg_hook = PostgresHook(conn_id)

    query = '''
    INSERT INTO {table}
    SELECT
        snap.starttimemillis,
        p.serviceName,
        p.entityid,
        'paid_transaction',
        p.servicetransactionid,
        snap.orderid,
        snap.productid,
        p.productname,
        snap.expirytimemillis
    FROM
        frog.purchases p
    JOIN
        production.android_subscription_snapshot snap on p.servicetransactionid = snap.purchasetoken
    WHERE
        snap.paymentstate = 1;

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
    get_ios_cancellation_events_task = PythonOperator(
        task_id='get_ios_cancellation_events',
        python_callable=get_ios_cancellation_events,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )
    finish_ios_task = DummyOperator(
        task_id='finish_ios'
    )

    get_android_free_trial_events_task = PythonOperator(
        task_id='get_android_free_trial_events',
        python_callable=get_android_free_trial_events,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )
    get_android_payment_events_task = PythonOperator(
        task_id='get_android_payment_events',
        python_callable=get_android_payment_events,
        op_kwargs={'conn_id': 'analytics_redshift'},
        provide_context=True
    )
    finish_android_task = DummyOperator(
        task_id='finish_android'
    )

    start_task >> drop_create_revenue_table_task >> [
        get_ios_free_trial_events_task, get_ios_payment_failed_events_task,
        get_ios_payment_events_task, get_ios_cancellation_events_task, ] >> finish_ios_task >> [
        get_android_free_trial_events_task, get_android_payment_events_task] >> finish_android_task
