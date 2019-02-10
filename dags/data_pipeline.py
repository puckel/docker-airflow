# This DAG has the goal to simulate a simple ETL data pipeline
# Sample data: Watson Analytics Sample Data – Sales Products
# https://www.ibm.com/communities/analytics/watson-analytics-blog/sales-products-sample-data/

import logging

from datetime import timedelta

from airflow import DAG
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from project.python_scripts.data_transformer import ETLoader


logger = logging.getLogger(__name__)


# default_args when passed to a DAG, it will apply to any of its operators
default_args = {
                "depends_on_past": False,
                "schedule_interval": "@once",
                "start_date": days_ago(1),
                "email": ["airflow_mail@no_active_server.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 1,
                "retry_delay": timedelta(minutes=5)
                }


dag = DAG('data_pipeline',
          description='Simple Data Pipeline',
          default_args=default_args
          )


with dag:

    # Variables are stored in the Metadata DB (note: any call to variables is a DB connection)
    revenue_table = Variable.get("revenue_table")

    # It's possible to store variables with JSON as value
    var_path_dir = Variable.get("path_dir", deserialize_json=True)

    # download_path_dir is /usr/local/airflow/project/downloads
    downloads_path_dir = var_path_dir["download_path_dir"]

    # bin_path_dir /usr/local/airflow/project/bin
    bin_path_dir = var_path_dir["bin_path_dir"]


    # Dummy operator, it's the "mock" for the download files step
    mock_download = DummyOperator(task_id='download_files',
                                  depends_on_past=False
                                  )


    # Get one random file from the downloads directory. Save in XCom the filename
    get_random_file = "cd {} && shuf -ezn 1 * | xargs -0 -n1 echo".format(downloads_path_dir)

    get_file = BashOperator(task_id="get_file",
                            bash_command=get_random_file,
                            # Save the filename in Xcom
                            xcom_push=True,
                            provide_context=True
                            )

    # use sqlite_conn_id to get the host '/usr/local/airflow/project/python_scripts/revenues.db'
    ddl_create_table = SqliteOperator(task_id="ddl_create_table",
                                      sql="""
                                            CREATE TABLE IF NOT EXISTS {}(
                                            year INTEGER,
                                            quarter	INTEGER, 
                                            retailer_country TEXT,
                                            retailer_type TEXT,	
                                            order_method_type TEXT,
                                            revenue INTEGER)
                                      ;""".format(revenue_table),
                                      sqlite_conn_id='sqlite_db'
                                      )


    def etl_data(**kwargs):
        # Extract Transform Load csv in DB
        task_instance = kwargs['ti']
        filename = task_instance.xcom_pull(task_ids='get_file')

        conn_host = SqliteHook(sqlite_conn_id='sqlite_db', supports_autocommit = True).get_conn()

        loader = ETLoader(filename, downloads_path_dir, conn_host, 'revenues', revenue_table)
        loader.orchestrate_etl()

        return filename

    # Extract from csv, Transform via Pandas DF, Load data into SQLite local DB
    etl_data_task = PythonOperator(task_id='ETL_data',
                                   python_callable=etl_data,
                                   provide_context=True,
                                   xcom_push=True
                                   )


    move_file_cmd = 'mv {}{}{{{{ ti.xcom_pull(task_ids="ETL_data") }}}} {}/.'\
                    .format(downloads_path_dir, '/', bin_path_dir)

    archive_file = BashOperator(task_id="archive_file",
                                bash_command=move_file_cmd,
                                provide_context=True
                                )


    mock_download >> get_file >> ddl_create_table >> etl_data_task >> archive_file
