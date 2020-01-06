from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


def refresh_all_teacher_table(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
    CREATE TABLE airflow_test.all_teacher_refresh (LIKE all_teacher);

    INSERT INTO
        airflow_test.all_teacher_refresh
    SELECT entity_id, email_address, title, first_name, last_name, school_id, timezone, locale, quiet_hours, marketing_email_opt_out
    FROM airflow_test.vanilla_teacher;

    INSERT INTO airflow_test.all_teacher_refresh
    SELECT entity_id, email_address, title, first_name, last_name, school_id, timezone, locale, quiet_hours, marketing_email_opt_out
    FROM airflow_test.mentor;

    INSERT INTO airlow_test.all_teacher_refresh
    SELECT entity_id, email_address, title, first_name, last_name, school_id, timezone, locale, quiet_hours, marketing_email_opt_out
    FROM airflow_test.school_leader;

    BEGIN read write;

    LOCK TABLE airflow_test.all_teacher;

    ALTER TABLE airflow_test.all_teacher RENAME TO all_teacher_old;
    ALTER TABLE airflow_test.all_teacher_refresh RENAME TO all_teacher;
    DROP TABLE airflow_test.all_teacher_old CASCADE;

    COMMIT;

    END TRANSACTION;
    '''
    pg_hook.run(sql)

def create_refresh_all_teacher_table_task(dag):
    t = PythonOperator(
        task_id="refresh_all_teacher_table",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=refresh_all_teacher_table,
        dag=dag
    )
    return t
