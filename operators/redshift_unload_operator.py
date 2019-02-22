from airflow.operators import PostgresOperator
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
        print(f'CONTEXT! {context}')
        bucket = self.custom_opts.get('bucket') or f'calm-redshift-dev'
        default_key = f"unloads/{context.get('dag_id')}/{context.get('task_id')}/{context.get('execution_date')}"
        key = self.custom_opts.get('key') or default_key
        self.custom_opts.update({'s3_location': f's3://{bucket}/{key}/'})
        self.opts.update(self.custom_opts)
        aws_key = self.opts.get('aws_access_key_id')
        aws_sec = self.opts.get('aws_secret_access_key')

        self.sql = f"""
            UNLOAD(
                '{self.opts.get('query')}'
            )
            to '{self.opts.get('s3_location')}'
            credentials 'aws_access_key_id={aws_key};aws_secret_access_key={aws_sec}'
            DELIMITER AS '{self.opts.get('delimiter')}'
            PARALLEL {self.opts.get('parallel')}
            {'HEADER' if self.opts.get('header') else ''}
            {'gzip' if self.opts.get('gzip') else ''}
            {'manifest' if self.opts.get('manifest') else ''};
        """
        super(RedshiftUnloadOperator, self).execute(context)
