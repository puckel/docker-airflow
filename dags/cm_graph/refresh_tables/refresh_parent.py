from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


def refresh_parent_table(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)

    sql = '''
    CREATE TABLE IF NOT EXISTS airflow_test.parent (LIKE parent);
    CREATE TABLE IF NOT EXISTS airflow_test.parent_refresh (LIKE parent);

    INSERT INTO
      airflow_test.parent_refresh
    WITH u AS (
      SELECT u.* FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY userId ORDER BY lastUpdated DESC) as row_num, *
        FROM production.user
      ) u WHERE row_num = 1
    ),
    up AS (
      SELECT user_parent.* FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY userId ORDER BY lastUpdated DESC) as row_num, *
        FROM production.user_parent
      ) user_parent WHERE row_num = 1
    ),
    p AS (
      SELECT parent.* FROM (
        SELECT ROW_NUMBER() OVER (PARTITION BY parentId ORDER BY lastUpdated DESC) as row_num, *
        FROM production.parent
      ) parent WHERE row_num =1
    )
    SELECT * FROM (
      SELECT
        parent.entity_id,
        parent.updated_at,
        parent.email_address,
        parent.first_name,
        parent.last_name,
        parent.locale,
        parent.timezone,
        parent.marketing_email_opt_out
      FROM
      (
        SELECT
          LOWER(SUBSTRING(parentid,1,24)) AS entity_id,
          GETDATE() AS updated_at,
          emailaddress AS email_address,
          CAST(firstname AS VARCHAR(100)) AS first_name,
          CAST(lastname AS VARCHAR(100)) AS last_name,
          (CASE WHEN locale IS NULL THEN 'en-US' ELSE locale END) AS locale,
          (CASE WHEN timezone IS NULL THEN 'America/Los_Angeles' ELSE timezone END) AS timezone,
          NULL as quiet_hours,
          CAST(marketingEmailOptOut as BOOLEAN) AS marketing_email_opt_out
        FROM u
        INNER JOIN up USING(userid)
        INNER JOIN p USING(parentid)
        WHERE u.deleted = 0
        AND p.deleted = 0
      ) AS parent
    )
    ;

    BEGIN read write;

    LOCK TABLE airflow_test.parent;

    ALTER TABLE airflow_test.parent RENAME to parent_old;
    ALTER TABLE airflow_test.parent_refresh RENAME to parent;
    DROP TABLE airflow_test.parent_old CASCADE;

    COMMIT;

    END TRANSACTION;
    '''
    pg_hook.run(sql)

def create_refresh_parent_table_task(dag):
    t = PythonOperator(
        task_id="refresh_parent_table",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=refresh_parent_table,
        dag=dag
    )
    return t
