from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


def refresh_invite_table(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)

    sql = '''
    CREATE TABLE IF NOT EXISTS airflow_test.invite (LIKE public.invite);
    CREATE TABLE IF NOT EXISTS airflow_test.invite_refresh (LIKE invite);

    INSERT INTO
      airflow_test.invite_refresh
    SELECT
      invite.invite_id,
      invite.student_id,
      invite.teacher_id,
      invite.parent_code,
      invite.status,
      invite.email_address,
      invite.phone_number,
      invite.created_at
    FROM
    (
    SELECT * FROM (
      SELECT
        ROW_NUMBER() OVER (PARTITION by email_address ORDER BY created_at DESC) as email_row_num,
        *
      FROM
      (
        SELECT * FROM
        (
          SELECT
            ROW_NUMBER() OVER (PARTITION BY invite_id ORDER BY created_at DESC) AS row_num,
            invite_id,
            student_id,
            teacher_id,
            parent_code,
            status,
            email_address,
            phone_number,
            is_deleted,
            created_at
          FROM
            public.invite_raw
        ) as named_subquery
        WHERE row_num = 1
        AND is_deleted = FALSE
      ) as anotherNamedSubquery
    ) as yetAnotherNamedSubquery
    WHERE email_row_num = 1
    ) AS invite
    ;

    BEGIN read write;

    LOCK TABLE airflow_test.invite;

    ALTER TABLE airflow_test.invite RENAME TO invite_old;
    ALTER TABLE airflow_test.invite_refresh RENAME TO invite;
    DROP TABLE airflow_test.invite_old CASCADE;

    COMMIT;

    END TRANSACTION;
    '''
    pg_hook.run(sql)

def create_refresh_invite_table_task(dag):
    t = PythonOperator(
        task_id="refresh_invite_table",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=refresh_invite_table,
        dag=dag
    )
    return t
