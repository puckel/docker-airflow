from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


def refresh_mentor_table(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
    CREATE TABLE IF NOT EXISTS airflow_test.mentor (LIKE public.mentor);
    CREATE TABLE IF NOT EXISTS airflow_test.mentor_refresh (LIKE public.mentor);

    INSERT INTO airflow_test.mentor_refresh
    WITH u AS (
      SELECT u.* FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY userId ORDER BY lastUpdated DESC) as row_num, *
        FROM production.user
      ) u WHERE row_num = 1
    ),
    user_teacher AS (
      SELECT user_teacher.* FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY userId ORDER BY lastUpdated DESC) as row_num, *
        FROM production.user_teacher
      ) user_teacher WHERE row_num = 1
    ),
    teacher AS (
      SELECT teacher.* FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY teacherId ORDER BY lastUpdated DESC) as row_num, *
        FROM production.teacher
      ) teacher WHERE row_num =1
    ),
    user_quiet_hours AS (
      SELECT user_quiet_hours.* FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY userId ORDER BY lastUpdated DESC) as row_num, *
        FROM production.user_quiet_hours
      ) user_quiet_hours WHERE row_num = 1
    ),
    school_teacher AS (
      SELECT school_teacher.* FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY teacherId, schoolId ORDER BY lastUpdated DESC) as row_num, *
        FROM production.school_teacher
      ) school_teacher WHERE row_num = 1
    )
    SELECT
      entity_id,
      updated_at,
      email_address,
      title,
      first_name,
      last_name,
      locale,
      timezone,
      school_id,
      is_verified,
      quiet_hours,
      marketing_email_opt_out
    FROM (
      SELECT
        LOWER(SUBSTRING(teacher.teacherId,1,24)) AS entity_id,
        CAST(title AS VARCHAR(50)) AS title,
        CAST(firstname AS VARCHAR(100)) AS first_name,
        CAST(lastname AS VARCHAR(100)) AS last_name,
        emailaddress AS email_address,
        LOWER(SUBSTRING(schoolid,1,24)) AS school_id,
        CAST(verified AS BOOLEAN) AS is_verified,
        CAST(mentor AS BOOLEAN) is_mentor,
        GETDATE() AS updated_at,
        (CASE WHEN locale IS NULL THEN 'en-US' ELSE locale END) AS locale,
        (CASE WHEN timezone IS NULL THEN 'America/Los_Angeles' ELSE timezone END) AS timezone,
        (CASE WHEN role IN ('principal','assistant_principal','school_leader') THEN role ELSE 'teacher' END) AS role,
        quietHours AS quiet_hours,
        CAST(marketingEmailOptOut as BOOLEAN) AS marketing_email_opt_out
      FROM u
      INNER JOIN user_teacher USING (userid)
      INNER JOIN teacher USING (teacherid)
      LEFT OUTER JOIN user_quiet_hours USING(userId)
      LEFT OUTER JOIN school_teacher
        ON (
          (school_teacher.archived IS NULL OR school_teacher.archived<>true)
          AND
          teacher.teacherId=school_teacher.teacherId
        )
        WHERE u.deleted = 0
        AND teacher.deleted = 0
    ) mentors
    WHERE is_mentor = true
    AND is_verified = true;

    BEGIN read write;

    LOCK TABLE airflow_test.mentor;

    ALTER TABLE airflow_test.mentor RENAME TO mentor_old;
    ALTER TABLE airflow_test.mentor_refresh RENAME TO mentor;
    DROP TABLE airflow_test.mentor_old CASCADE;

    COMMIT;

    END TRANSACTION;

    '''
    pg_hook.run(sql)

def create_refresh_mentor_table_task(dag):
    t = PythonOperator(
        task_id="refresh_mentor_table",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=refresh_mentor_table,
        dag=dag
    )
    return t
