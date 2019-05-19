from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import airflowlib.emr_lib as emr

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 5, 16),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "end_date": datetime(2021, 1, 1),
}

dag = DAG("emrManager", default_args=default_args, schedule_interval=timedelta(1))


emr = emr.client('EMRConnection')
region = 'eu-west-1'

# Creates an EMR cluster
def createCluster(**kwargs):
    cluster_id = emr.create_cluster(region_name=region, cluster_name='airflowCluster', num_core_nodes=2)
    return cluster_id

def waitForClusterCompletion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='createCluster')
    emr.wait_for_cluster_creation(cluster_id)

# Terminates the EMR cluster
def terminateCluster(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='createCluster')
    emr.terminate_cluster(cluster_id)

def transformData(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='createCluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'spark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                              '/root/airflow/dags/transform/transformData.scala')
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)

tCreateCluster = PythonOperator(
    task_id='createCluster',
    python_callable=createCluster,
    dag=dag)

tWaitForClusterCompletion = PythonOperator(
    task_id='waitForClusterCompletion',
    python_callable=waitForClusterCompletion,
    dag=dag)

tTerminateCluster = PythonOperator(
task_id='terminateCluster',
    python_callable=terminateCluster,
    trigger_rule='all_done',
    dag=dag)

tTransformData = PythonOperator(
    task_id='transformData',
    python_callable=transformData,
    dag=dag)


tCreateCluster >> tWaitForClusterCompletion
tWaitForClusterCompletion >> tTransformData  >> tTerminateCluster

