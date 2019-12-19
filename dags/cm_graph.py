from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook
from datetime import timedelta


dag = DAG("Campaign Manager Events from Raw",
          description="Recreate views and events that CM reads from"
          schedule_interval=timedelta(minutes=30),
          concurrency=5,
          dagrun_timeout=timedelta(minutes=60),
          max_active_runs=1,
          catchup=False,
          on_failure_callback=failure_callback)


def failure_callback(ctx):
    print("FAILED")
    # Add PD

def create_cache_teacher_view(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
        CREATE OR REPLACE VIEW
            airflow_test.cache_teacher
        AS
        (
            SELECT
                first_name as firstname,
                last_name as lastname,
                email_address as emailaddress,
                (UPPER(entity_id)) as teacherid
            FROM
                public.all_teacher
        );

    '''
    pg_hook.run(sql)

def create_cache_parent_view(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
        CREATE OR REPLACE VIEW
            airflow_test.cache_parent
        AS
        (
            SELECT
                first_name as firstname,
                last_name as lastname,
                email_address as emailaddress,
                (UPPER(entity_id)) as parentid
            FROM
                public.parent
        )
    '''

t1 = PythonOperator(
    task_id="create_cache_teacher",
    op_kwargs={'conn_id': 'campaign_manager_redshift'},
    python_callable=create_cache_teacher_view,
    dag=dag
)

t2 = PythonOperator(
    task_id="create_cache_parent",
    op_kwargs={'conn_id': 'campaign_manager_redshift'},
    python_callable=create_cache_parent_view,
    dag=dag

)
