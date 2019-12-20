from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


def create_schoolleader_lifecycle_event_view(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
    CREATE OR REPLACE VIEW
      airflow_test.school_leader_lifecycle_event
    AS (
    SELECT
      evt.entity_id,

      -- school leader activation events
      MAX(CASE WHEN evt.type = 'Lifecycle opt in'                THEN evt.time   ELSE NULL END) AS opt_in,
      MAX(CASE WHEN evt.type = 'Teacher: signed up'              THEN evt.time   ELSE NULL END) AS signed_up,
      MAX(CASE WHEN evt.type = 'Teacher: joined school'          THEN evt.time   ELSE NULL END) AS joined_school,
      MAX(CASE WHEN evt.type = 'Teacher: declined from school'   THEN evt.time   ELSE NULL END) AS declined,

      -- school leader activation emails
      MAX(CASE WHEN email.name = 'school_leader_join_school'           THEN email.time ElSE NULL END) AS join_school_email,
      MAX(CASE WHEN email.name = 'school_leader_get_verified_0'        THEN email.time ElSE NULL END) AS get_verified_0_email,
      MAX(CASE WHEN email.name = 'school_leader_get_verified_1'        THEN email.time ElSE NULL END) AS get_verified_1_email,
      MAX(CASE WHEN email.name = 'school_leader_verified'              THEN email.time ElSE NULL END) AS verified_email,
      MAX(CASE WHEN email.name = 'school_leader_declined'              THEN email.time ElSE NULL END) AS declined_email,
      MAX(CASE WHEN email.name = 'school_leader_welcome_verified'      THEN email.time ElSE NULL END) AS welcome_verified_email,
      MAX(CASE WHEN email.name = 'school_leader_welcome_unverified'    THEN email.time ElSE NULL END) AS welcome_unverified_email,
      MAX(email.time)                                                                                 AS last_email

    FROM
    (
      SELECT
        entity_id, type, MAX(created_at) AS time
      FROM
        public.event
      GROUP BY
        entity_id, type
    ) AS evt
    LEFT JOIN
      public.campaign_history AS email
    ON
      email.entity_id = evt.entity_id
    GROUP BY
      evt.entity_id
    )
    '''
    pg_hook.run(sql)


def create_schoolleader_lifecycle_view_task(dag):
    t = PythonOperator(
        task_id="create_schoolleader_lifecycle_event_view",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=create_schoolleader_lifecycle_event_view,
        dag=dag
    )
    return t
