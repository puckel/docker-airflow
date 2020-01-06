from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


def refresh_school_table(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
    CREATE TABLE airflow_test.school (LIKE school);
    CREATE TABLE airflow_test.school_refresh (LIKE school);

    INSERT INTO
      airflow_test.school_refresh
    SELECT
      LOWER(SUBSTRING(schoolid,1,24)) AS school_id,
      GETDATE() as updated_at,
      SUBSTRING(name, 1, 240) AS name
    FROM
      production.school
    ;

    BEGIN read write;

    ALTER TABLE airflow_test.school RENAME TO school_old;
    ALTER TABLE airflow_test.school_refresh RENAME TO school;
    DROP TABLE airflow_test.school_old CASCADE;

    COMMIT;

    END TRANSACTION;
    '''
    pg_hook.run(sql)

def create_refresh_school_table_task(dag):
    t = PythonOperator(
        task_id="refresh_school_table",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=refresh_school_table,
        dag=dag
    )
    return t
