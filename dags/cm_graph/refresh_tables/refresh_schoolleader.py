from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


def refresh_schoolleader_table(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
    CREATE TABLE airflow_test.school_leader (LIKE public.school_leader);
    CREATE TABLE airflow_test.school_leader_refresh (LIKE public.school_leader);

    INSERT INTO
      airflow_test.school_leader_refresh
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
        SELECT ROW_NUMBER() OVER (PARTITION BY teacherId + schoolId ORDER BY lastUpdated DESC) as row_num, *
        FROM production.school_teacher
      ) school_teacher WHERE row_num = 1
    )
    SELECT
      teacher.entity_id,
      teacher.updated_at,
      teacher.email_address,
      teacher.title,
      teacher.first_name,
      teacher.last_name,
      teacher.locale,
      teacher.timezone,
      teacher.school_id,
      role,
      (CASE WHEN mentor_school_id IS NOT NULL THEN true ELSE false END) AS has_mentor,
      mentor_first_name,
      mentor_last_name,
      teacher.is_verified,
      quiet_hours,
      marketing_email_opt_out
    FROM (

    SELECT
      emailaddress AS email_address,
      CAST((CASE WHEN firstname IS NOT NULL then firstname ELSE '' END) AS VARCHAR(100)) AS first_name,
      CAST((CASE WHEN lastname IS NOT NULL then lastname ELSE '' END) AS VARCHAR(100)) AS last_name,
      CAST(title AS VARCHAR(50)) as title,

      LOWER(SUBSTRING(teacher.teacherid,1,24)) AS entity_id,
      LOWER(SUBSTRING(schoolid,1,24)) AS school_id,
      CAST(verified AS BOOLEAN) as is_verified,
      CAST(mentor AS BOOLEAN) is_mentor,

      GETDATE() AS updated_at,
      (CASE WHEN locale IS NULL THEN 'en-US' ELSE locale END) AS locale,
      (CASE WHEN timezone IS NULL THEN 'America/Los_Angeles' ELSE timezone END) AS timezone,
      role,
      quiethours AS quiet_hours,
      CAST(marketingEmailOptOut as BOOLEAN) AS marketing_email_opt_out
    FROM u
    INNER JOIN user_teacher USING(userid)
    INNER JOIN teacher USING(teacherid)
    LEFT OUTER JOIN user_quiet_hours USING(userid)
    LEFT OUTER JOIN school_teacher
      ON (
        (school_teacher.archived IS NULL OR school_teacher.archived<>true)
        AND
        teacher.teacherId=school_teacher.teacherId
      )
    WHERE
      u.deleted=0 AND
      teacher.deleted=0 AND
      (school_teacher.archived IS NULL OR school_teacher.archived<>true)
    ) teacher
    LEFT JOIN
    (
      SELECT * FROM
      (
        SELECT
          ROW_NUMBER() OVER (PARTITION BY school_id ORDER BY updated_at DESC) AS rank,
          school_id as mentor_school_id,
          (CASE WHEN first_name IS NOT NULL THEN first_name ELSE '' END) as mentor_first_name,
          (CASE WHEN last_name IS NOT NULL THEN last_name ELSE '' END) as mentor_last_name
        FROM
            airflow_test.mentor
        WHERE
            airflow_test.mentor.is_verified=true
      ) AS subqueryName1
      WHERE rank = 1
    ) AS m
    ON teacher.school_id = m.mentor_school_id
    WHERE
      (is_mentor <> true OR is_mentor IS NULL) AND
      role IN ('principal', 'assistant_principal', 'school_leader')
    ;

    BEGIN read write;


    LOCK TABLE airflow_test.school_leader;
    ALTER TABLE airflow_test.school_leader RENAME TO school_leader_old;
    ALTER TABLE airflow_test.school_leader_refresh RENAME TO school_leader;
    DROP TABLE airflow_test.school_leader_old CASCADE;

    COMMIT;

    END TRANSACTION;
    '''
    pg_hook.run(sql)

def create_refresh_schoolleader_table_task(dag):
    t = PythonOperator(
        task_id="refresh_schoolleader_table",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=refresh_schoolleader_table,
        dag=dag
    )
    return t
