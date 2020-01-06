from airflow.operators.python_operator import PythonOperator
from airflow.hooks import PostgresHook


def refresh_parent_connection_request_table(**kwargs):
    conn_id = kwargs.get('conn_id')
    pg_hook = PostgresHook(conn_id)
    sql = '''
    CREATE TABLE IF NOT EXISTS airflow_test.parent_connection_request (LIKE parent_connection_request);
    CREATE TABLE IF NOT EXISTS airflow_test.parent_connection_request_refresh (LIKE parent_connection_request);

    INSERT INTO
      airflow_test.parent_connection_request_refresh
    SELECT
      LOWER(SUBSTRING(requestId,1,24)) as request_id,
      LOWER(SUBSTRING(teacherId,1,24)) as teacher_id,
      LOWER(SUBSTRING(parentId,1,24)) as parent_id,
      createdAt as created_at,
      GETDATE() as updated_at,
      acceptedAt as accepted_at,
      CASE WHEN studentName IS NOT NULL THEN studentName ELSE student.firstName + ' ' + student.lastName END as student_name,
      LOWER(SUBSTRING(acceptedStudentId,1,24)) as accepted_student_id,
      pcr.deleted,
      deletedByParent = 1 as deleted_by_parent,
      LOWER(SUBSTRING(studentId,1,24)) as student_id
    FROM production.parent_connection_request pcr
    LEFT JOIN production.student student USING(studentId)
    WHERE student.deleted = false
    AND pcr.parentId IS NOT NULL;
    ;

    BEGIN read write;

    LOCK TABLE airflow_test.parent_connection_request;

    ALTER TABLE airflow_test.parent_connection_request RENAME TO parent_connection_request_old;
    ALTER TABLE airflow_test.parent_connection_request_refresh RENAME TO parent_connection_request;
    DROP TABLE airflow_test.parent_connection_request_old CASCADE;

    COMMIT;

    END TRANSACTION;
    '''
    pg_hook.run(sql)

def create_refresh_parent_connection_request_table_task(dag):
    t = PythonOperator(
        task_id="refresh_parent_connection_request_table",
        op_kwargs={'conn_id': 'campaign_manager_redshift'},
        python_callable=refresh_parent_connection_request_table,
        dag=dag
    )
    return t
