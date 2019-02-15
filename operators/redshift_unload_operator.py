from airflow.operators import PostgresOperator
from airflow.exceptions import AirflowException
import os


class RedshiftUnloadOperator(PostgresOperator):
    opts = {
        'delimiter': '\t',
        'parallel': True,
        'header': True,
        'gzip': True,
        'manifest': True,
        'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY')
    }
    # use a sql string here until we get a sandbox going for testing template rendering

    def __init__(self, custom_unload_options, postgres_conn_id, db, task_id, dag):
        super(RedshiftUnloadOperator, self).__init__(sql=None, postgres_conn_id=postgres_conn_id,
                                                     autocommit=True, database=db, task_id=task_id, dag=dag)

        self.custom_opts = custom_unload_options


    def execute(self, context):
        # ENV = os.getenv('CALM-ENV')
        bucket = self.custom_opts.get('bucket') or f'calm-redshift-dev'
        key = self.custom_opts.get('key') or f'unloads/{context.dag}/{context.task_id}/{context.execution_date}'
        self.custom_opts.update({ 's3_location': f's3://{bucket}/{key}/' })
        self.opts.update(self.custom_opts)

        self.sql = f"""
            UNLOAD(
                '{self.opts.query}'
            )
            to '{self.opts.s3_location}'
            credentials 'aws_access_key_id={self.opts.aws_access_key_id};aws_secret_access_key={self.opts.aws_secret_access_key}'
            DELIMITER AS '{self.opts.delimiter}'
            PARALLEL {self.opts.parallel}
            {'HEADER' if self.opts.header else ''}
            {'gzip' if self.opts.gzip else ''}
            {'manifest' if self.opts.manifest else ''};
        """

        super(RedshiftUnloadOperator).execute(context)
