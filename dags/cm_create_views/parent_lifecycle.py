from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


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

def create_parent_lifecycle_view_task(dag):
    t = PythonOperator(
        task_id="create_parent_lifecycle_event_view",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=create_parent_lifecycle_event_view,
        dag=dag
    )
    return t
