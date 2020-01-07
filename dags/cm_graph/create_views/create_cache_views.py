from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


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
                airflow_test.all_teacher
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
                airflow_test.parent
        )
    '''
    pg_hook.run(sql)

def create_cache_schoolleader_mentor(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
    CREATE OR REPLACE VIEW airflow_test.verified_school_leader_and_mentor AS
    (
      SELECT * FROM
      (
        SELECT
          entity_id,title,first_name,last_name,school_id
        FROM airflow_test.school_leader
        WHERE is_verified=true
        UNION
        SELECT
          entity_id,title,first_name,last_name,school_id
        FROM airflow_test.mentor
        WHERE is_verified=true
      ) AS subquery
      ORDER BY entity_id DESC
    )

    '''
    pg_hook.run(sql)

def create_cache_teacher_view_task(dag):
    t = PythonOperator(
        task_id="create_cache_teacher",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=create_cache_teacher_view,
        dag=dag
    )
    return t

def create_cache_parent_view_task(dag):
    t = PythonOperator(
        task_id="create_cache_parent",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=create_cache_parent_view,
        dag=dag
    )
    return t

def create_schoolleader_mentor_view_task(dag):
    t = PythonOperator(
        task_id="create_cache_schoolleader_mentor",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=create_cache_schoolleader_mentor,
        dag=dag
    )
    return t

