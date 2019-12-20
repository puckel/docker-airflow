from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


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

def create_teacher_lifecycle_view_task(dag):
    t = PythonOperator(
        task_id="create_teacher_lifecycle_event_view",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=create_teacher_lifecycle_event_view,
        dag=dag
    )

    return t
