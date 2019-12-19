from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook
from datetime import timedelta, datetime


def failure_callback(ctx):
    print("FAILED")
    # Add PD

dag = DAG("campaign_manager_refresh_events",
          description="Recreate views and events that CM reads from",
          schedule_interval=timedelta(minutes=30),
          concurrency=5,
          dagrun_timeout=timedelta(minutes=60),
          max_active_runs=1,
          catchup=False,
          start_date=datetime(2019, 12, 19),
          on_failure_callback=failure_callback)

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
    pg_hook.run(sql)

def create_mentor_lifecycle_event_view(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
        CREATE OR REPLACE VIEW
            airflow_test.mentor_lifecycle_event
        AS
        (
            SELECT
                evt.entity_id,

                -- mentor activation events
                MAX(CASE WHEN evt.type = 'Teacher: became mentor'  THEN evt.time   ELSE NULL END) AS became_mentor,

                -- mentor activation emails
                MAX(CASE WHEN email.name = 'mentor_welcome'        THEN email.time ElSE NULL END) AS mentor_welcome_email,
                MAX(CASE WHEN email.name = 'mentor_join_community' THEN email.time ElSE NULL END) AS mentor_join_community_email,
                MAX(CASE WHEN email.name = 'mentor_tip_1'          THEN email.time ElSE NULL END) AS mentor_tip_1_email,
                MAX(CASE WHEN email.name = 'mentor_tip_2'          THEN email.time ElSE NULL END) AS mentor_tip_2_email,
                MAX(CASE WHEN email.name = 'mentor_tip_3'          THEN email.time ElSE NULL END) AS mentor_tip_3_email,
                MAX(CASE WHEN email.name = 'mentor_tip_4'          THEN email.time ElSE NULL END) AS mentor_tip_4_email,

                MAX(email.time)                                                                   AS last_email
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

def create_parent_lifecycle_event_view(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
        CREATE OR REPLACE VIEW
          airflow_test.parent_lifecycle_event
        AS
        (
        SELECT
          evt.entity_id,
          MAX(CASE WHEN evt.type = 'Lifecycle opt in'                   THEN evt.time   ELSE NULL END) AS opt_in,
          MAX(CASE WHEN evt.type = 'Parent: signed up'                  THEN evt.time   ELSE NULL END) AS signed_up,
          MAX(CASE WHEN evt.type = 'Parent: logged in'                  THEN evt.time   ELSE NULL END) AS logged_in,
          MAX(CASE WHEN evt.type = 'Parent: joined class' OR evt.type = 'Parent: received point' THEN evt.time   ELSE NULL END) AS joined_class,
          MAX(CASE WHEN evt.type = 'Parent: second parent joined class' THEN evt.time   ELSE NULL END) AS second_parent_joined_class,
          MAX(CASE WHEN evt.type = 'Parent: used mobile app'            THEN evt.time   ELSE NULL END) AS used_mobile,
          MAX(CASE WHEN evt.type = 'Parent: received point'             THEN evt.time   ELSE NULL END) AS received_point,

          MAX(CASE WHEN email.name = 'parent_welcome'                   THEN email.time ElSE NULL END) AS welcome_email,
          MAX(CASE WHEN email.name = 'parent_join_class_0'              THEN email.time ElSE NULL END) AS join_class_0_email,
          MAX(CASE WHEN email.name = 'parent_join_class_1'              THEN email.time ElSE NULL END) AS join_class_1_email,
          MAX(CASE WHEN email.name = 'parent_get_the_app'               THEN email.time ElSE NULL END) AS get_the_app_email,
          MAX(CASE WHEN email.name = 'parent_invite_spouse'             THEN email.time ElSE NULL END) AS invite_spouse_email,
          MAX(CASE WHEN email.name = 'parent_retention'                 THEN email.time ElSE NULL END) AS parent_retention_email,

          MAX(email.time)                                                                              AS last_email
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

def create_teacher_lifecycle_event_view(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
    CREATE OR REPLACE VIEW
      airflow_test.teacher_lifecycle_event
    AS
    (
    SELECT
      evt.entity_id,

      -- teacher activation events
      MAX(CASE WHEN evt.type = 'Lifecycle opt in'            THEN evt.time   ELSE NULL END) AS opt_in,
      MAX(CASE WHEN evt.type = 'Teacher: signed up'          THEN evt.time   ELSE NULL END) AS signed_up,
      MAX(CASE WHEN evt.type = 'Teacher: added class' OR evt.type = 'Teacher: added_student' THEN evt.time   ElSE NULL END) AS added_class,
      MAX(CASE WHEN evt.type = 'Teacher: added student'      THEN evt.time   ElSE NULL END) AS added_student,
      MAX(CASE WHEN evt.type = 'Teacher: awarded point'      THEN evt.time   ElSE NULL END) AS awarded_point,
      MAX(CASE WHEN evt.type = 'Teacher: parent connected'   THEN evt.time   ElSE NULL END) AS invited_parents,
      MAX(CASE WHEN evt.type = 'Teacher: verified at school' THEN evt.time   ElSE NULL END) AS was_verified,
      MAX(CASE WHEN evt.type = 'Teacher: used mobile app'    THEN evt.time   ElSE NULL END) AS used_mobile,

      -- teacher activation emails
      MAX(CASE WHEN email.name = 'teacher_welcome'
        -- If a user switches back and forth, they should only get one of these welcomes
        OR email.name ='school_leader_welcome_verified'
        OR email.name= 'school_leader_welcome_unverified'
        THEN email.time ElSE NULL END
      ) AS welcome_email,
      MAX(CASE WHEN email.name = 'teacher_add_class_0'       THEN email.time ElSE NULL END) AS add_class_0_email,
      MAX(CASE WHEN email.name = 'teacher_add_class_1'       THEN email.time ElSE NULL END) AS add_class_1_email,
      MAX(CASE WHEN email.name = 'teacher_add_class_2'       THEN email.time ElSE NULL END) AS add_class_2_email,
      MAX(CASE WHEN email.name = 'teacher_add_class_3'       THEN email.time ElSE NULL END) AS add_class_3_email,
      MAX(CASE WHEN email.name = 'teacher_ideas'             THEN email.time ElSE NULL END) AS ideas_email,
      MAX(CASE WHEN email.name = 'teacher_join_school'       THEN email.time ElSE NULL END) AS join_school_email,
      MAX(CASE WHEN email.name = 'teacher_invite_parents_0'  THEN email.time ElSE NULL END) AS invite_parents_0_email,
      MAX(CASE WHEN email.name = 'teacher_invite_parents_1'  THEN email.time ElSE NULL END) AS invite_parents_1_email,
      MAX(CASE WHEN email.name = 'teacher_invite_parents_2'  THEN email.time ElSE NULL END) AS invite_parents_2_email,
      MAX(CASE WHEN email.name = 'teacher_invite_parents_3'  THEN email.time ElSE NULL END) AS invite_parents_3_email,
      MAX(CASE WHEN email.name = 'teacher_verification'      THEN email.time ElSE NULL END) AS get_verified_email,
      MAX(CASE WHEN email.name = 'teacher_get_the_app'       THEN email.time ElSE NULL END) AS get_the_app_email,

      -- teacher feature awareness events
      MAX(CASE WHEN evt.type = 'Teacher: used messaging'     THEN evt.time   ElSE NULL END) AS used_messaging,
      MAX(CASE WHEN evt.type = 'Teacher: used class story'   THEN evt.time   ElSE NULL END) AS used_class_story,
      MAX(CASE WHEN evt.type = 'Teacher: used school story'  THEN evt.time   ElSE NULL END) AS used_school_story,
      MAX(CASE WHEN evt.type = 'Teacher: used group'         THEN evt.time   ElSE NULL END) AS used_group,

      -- teacher feature awareness emails
      MAX(CASE WHEN email.name = 'teacher_groups'            THEN email.time ElSE NULL END) AS teacher_group_email,
      MAX(CASE WHEN email.name = 'teacher_resources'         THEN email.time ElSE NULL END) AS teacher_resources_email,
      MAX(CASE WHEN email.name = 'teacher_messaging' OR email.name = 'teacher_messaging_connected' THEN email.time ElSE NULL END) AS teacher_messaging_email,
      MAX(CASE WHEN email.name = 'teacher_class_story'       THEN email.time ElSE NULL END) AS teacher_class_story_email,
      MAX(CASE WHEN email.name = 'teacher_school_story'      THEN email.time ElSE NULL END) AS teacher_school_story_email,
      MAX(CASE WHEN email.name = 'teacher_big_ideas'         THEN email.time ElSE NULL END) AS teacher_big_ideas_email,

      MAX(email.time)                                                                       AS last_email
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
        FROM public.school_leader
        WHERE is_verified=true
        UNION
        SELECT
          entity_id,title,first_name,last_name,school_id
        FROM public.mentor
        WHERE is_verified=true
      ) AS subquery
      ORDER BY entity_id DESC
    )

    '''
    pg_hook.run(sql)

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

t3 = PythonOperator(
    task_id="create_mentor_lifecycle_event_view",
    op_kwargs={'conn_id': 'campaign_manager_redshift'},
    python_callable=create_mentor_lifecycle_event_view,
    dag=dag
)

t4 = PythonOperator(
    task_id="create_parent_lifecycle_event_view",
    op_kwargs={'conn_id': 'campaign_manager_redshift'},
    python_callable=create_parent_lifecycle_event_view,
    dag=dag
)

t5 = PythonOperator(
    task_id="create_schoolleader_lifecycle_event_view",
    op_kwargs={'conn_id': 'campaign_manager_redshift'},
    python_callable=create_schoolleader_lifecycle_event_view,
    dag=dag
)

t6 = PythonOperator(
    task_id="create_teacher_lifecycle_event_view",
    op_kwargs={'conn_id': 'campaign_manager_redshift'},
    python_callable=create_teacher_lifecycle_event_view,
    dag=dag
)

t7 = PythonOperator(
    task_id="create_cache_schoolleader_mentor",
    op_kwargs={'conn_id': 'campaign_manager_redshift'},
    python_callable=create_cache_schoolleader_mentor,
    dag=dag
)

