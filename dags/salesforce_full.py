from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.salesforce_plugin import SalesforceToFileOperator

from airflow.contrib.operators import file_to_gcs
from airflow.contrib.operators import gcs_to_bq

import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 22),
    'email': ['foo@bar.fom'], # Make this an ENV
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('salesforce_full',
          schedule_interval='*/5 * * * *',
          default_args=default_args)

output_filename = "account.json"
output_filepath = os.path.join(".", output_filename)

output_schemaname = "account_schema.json"
output_schema = os.path.join(".", output_schemaname)

# these values can and should change based on your unique setup
# the GCS BUCKET is the bucket on Google Cloud Storage where you want the files to live
# GCS_CONN_ID is the name of the Airflow connection that holds your credentials to connect to the Google API
# SF_CONN_ID is the name of the Airflow connection that holds your crednetials to connect to your Salesforce API
#
# To setup connections:
#   - launch the airflow webserver:
#       >$ airflow webserver
#   - Go to Admin->Connections and then click on "Create"
#   - select the corresponding connection type and enter in your info
#       - For Google Cloud Services use "Google Cloud Platform"
#       - For Salesforce use "HTTP"
#           * username:     your salesforce username
#           * passsword:    your salesforce password
#           * extra:        you should put a JSON structure here that contains your security token if your SF implemntation requires it
#               - {"security_token": "YOUR_SECURITY_TOKEN_HERE"}

# Todo: make this and anything else potentially org-related an environment variable
## TEMPORARILY REMOVED ## GCS_BUCKET = "gs://foo-bar-baz"
GCS_CONN_ID = "google_cloud_storage_default"
## TEMPORARILY REMOVED ## SF_CONN_ID = "quxyquxqux"

# query salesforce
# the SalesforceToFileOperator takes in a conection name
# to define the connection, go to Admin -> Connections
# it uses the HTTP connection type
# the security token is included in the "Extras" field, which allows you to define extra attributes in a JSON format
#   {"security_token":"asdasdasd"}
t1 = SalesforceToFileOperator(
    task_id =               "get_model_salesforce",
    obj =                   "Account",          # the object we are querying
    #fields =                ["Name", "Id"],    # you can use this to limit the fields that are queried
    conn_id =               SF_CONN_ID,         # name of the Airflow connection that has our SF credentials
    output =                output_filepath,    # file where the resulting data is stored
    output_schemafile =     output_schema,      # tell the operator that we want a file of the resulting schema
    output_schematype =     "BQ",               # specify that we want to generate a schema file for BigQuery
    fmt =                   "ndjson",           # write the file as newline deliminated json.  Other options include CSV and JSON
    record_time_added =     True,               # add a column to the output that records the time at which the data was fetched
    coerce_to_timestamp =   True,               # coerce all date and datetime fields into Unix timestamps (UTC)
    dag =                   dag
)

# push result to GCS
t2a = file_to_gcs.FileToGoogleCloudStorageOperator(
    task_id =   "model_to_gcs",
    dst =       output_filename,
    bucket =    GCS_BUCKET,
    conn_id =   GCS_CONN_ID,
    src =       output_filepath,
    dag =       dag
)

# push schema to GCS
t2b = file_to_gcs.FileToGoogleCloudStorageOperator(
    task_id =   "model_schema_to_gcs",
    dst =       output_schemaname,
    bucket =    GCS_BUCKET,
    conn_id =   GCS_CONN_ID,
    src =       output_schema,
    dag =       dag
)

# move to BigQuery
# Create and write disposition settings and descriptions
#   https://cloud.google.com/bigquery/docs/reference/v2/jobs
#
# Also, valid source formats are not made particularly clear, but this page describes them:
#   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.tableDefinitions.(key).sourceFormat
t3 = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
    task_id =           "file_to_bigquery",
    bucket =            GCS_BUCKET,
    source_objects =    [output_filename],
    schema_object =     output_schemaname,
    source_format =     "NEWLINE_DELIMITED_JSON",
    destination_project_dataset_table = "scratch.sf_account",
    create_disposition = "CREATE_IF_NEEDED",
    write_disposition = "WRITE_APPEND",
    dag = dag
)

# moving the files can happen at the same time as soon as the first task as finished
t1.set_downstream(t2a)
t1.set_downstream(t2b)

# moving the data with the appropriate schema can't happen until both of those files are uploaded to GCS
t3.set_upstream([t2a, t2b])
