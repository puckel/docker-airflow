from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


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

def create_mentor_lifecycle_view_task(dag):
    t = PythonOperator(
        task_id="create_mentor_lifecycle_event_view",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=create_mentor_lifecycle_event_view,
        dag=dag
    )
    return t
