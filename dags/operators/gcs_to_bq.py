from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

class MyGoogleCloudStorageToBigQueryOperator(GoogleCloudStorageToBigQueryOperator):

    def __init__(
        self,
        previous_task_id,
        *args,
        **kwargs
    ):
        self.previous_task_id = previous_task_id
        super().__init__(*args, **kwargs)

    def execute(self, context):
        self.schema_object = context['ti'].xcom_pull(task_ids=self.previous_task_id)['schema']
        self.source_objects = context['ti'].xcom_pull(task_ids=self.previous_task_id)['files']
        super().execute(context)
