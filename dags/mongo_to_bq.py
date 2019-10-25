
"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta

import airflow
import datetime
from airflow.models import DAG

from operators.mongo_to_gcs import MongoToGoogleCloudStorageOperator
from operators.gcs_to_bq import MyGoogleCloudStorageToBigQueryOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id='mongo_to_bq',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    start_date=datetime.datetime(2019, 10, 25)
)

find = {
    '_updated_at': {
        '$gte': datetime.datetime(2019, 10, 23)
    }
}
projection = ['_id', 'username', '_created_at', '_updated_at']
bq_field_type_map = {
    '_created_at': 'TIMESTAMP',
    '_updated_at': 'TIMESTAMP'
}
bucket = 'fitbod-airflow-test'
fileprefix = 'test1'
table = 'airflow_test.users'

users_to_gcs = MongoToGoogleCloudStorageOperator(
    task_id='users_to_gcs',
    dag=dag,
    collection='_User',
    find=find,
    projection=projection,
    bq_field_type_map=bq_field_type_map,
    bucket=bucket,
    filename=fileprefix,
    export_format='csv'
)

bq_load_users = MyGoogleCloudStorageToBigQueryOperator(
    task_id='bq_load_users', 
    previous_task_id='users_to_gcs',
    bucket=bucket,
    schema_object=fileprefix+"-schema.json",
    source_objects="{{ task_instance.xcom_pull(task_ids='users_to_gcs') }}",
    destination_project_dataset_table=table, 
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1
)

users_to_gcs >> bq_load_users

if __name__ == "__main__":
    dag.cli()
