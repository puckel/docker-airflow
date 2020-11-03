import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.providers.pagerduty.hooks.pagerduty import PagerdutyHook
from airflow.models import Variable

import pytz
from datetime import datetime, date

# Setup Date/Time Objects
pacific = pytz.timezone("US/Pacific")
today = datetime.now(pacific).strftime("%Y-%m-%d")
testdate = date(2020, 10, 15).strftime("%Y-%m-%d")

# DAG definition
args = {
    'owner': 'airflow',
    'email': ['nchmura@veraset.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}
dag = DAG(dag_id='test_dag', default_args=args)

# Success/Failure Handlers
def slack_success(context):
    print("running slack success") 
    success_alert = SlackWebhookOperator(
        task_id='slack_success_alert',
        http_conn_id='slack',
        message="Task {} Succeeded".format(context["ti"].task_id),
        channel='#sns_testing',
        username='Airflow',
        icon_emoji=':airflow:'
    )
    success_alert.execute(context=context)

def pagerduty_alert(context):
    pd = PagerdutyHook(
            pagerduty_conn_id='pagerduty'
            )
    return pd.create_event(
            summary="TEST Airflow Alert",
            severity='critical',
            custom_details='this is a test. hello airflow.'
            )

# Task Definitions
databricks_task = DatabricksRunNowOperator(
               task_id='run_databricks_job',
               dag=dag,
               on_success_callback=slack_success,
               job_id=22)

bash_task = BashOperator(
        task_id='run_bash_job',
        dag=dag,
        bash_command='echo hello veraset!',
        on_success_callback=slack_success,
        on_failure_callback=pagerduty_alert
        )

egress_task = SimpleHttpOperator(
    http_conn_id='veraset_egress_api',
    task_id='egress_test',
    endpoint='/copy',
    method='GET',
    data={"date": testdate, "customer": "veraset-egress-test1", "product": "MOVEMENT"},
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.json()['status'] == "SUBMITTED",
    dag=dag
)

# DAG Dependencies
databricks_task >> bash_task >> egress_task
