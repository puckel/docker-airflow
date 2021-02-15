from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#slack
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

from datetime import datetime, timedelta
from airflow.models import Variable

#XCOM abbreviation for cross communication
#push and pull


default_args = {
    'owner': 'learning',
    'depends_on_past': False,
    'start_date': datetime(2021, 2, 2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
}
dag = DAG(
    'Hello', default_args=default_args, schedule_interval="0 * * * *")

Stage1 = BashOperator(
    task_id='Hello',
    bash_command='echo {{ var.value.test}}',
    dag=dag)

Stage2 = BashOperator(
    task_id='World',
    bash_command='echo world',
    dag=dag)

#function to get the number 7
def seven():
    return 7

#First Way to push using xcom
Stage3 = PythonOperator(
     task_id = 'try_xcom7',
     python_callable = seven,
     xcom_push=True,
     dag = dag)

def pushnine(**context):
    context['ti'].xcom_push(key='keyNINE', value=9)

#second way to push
Stage5 = PythonOperator(
    task_id = 'push9',
    python_callable = pushnine,
    dag = dag
    )

def getNINE(**context):
    value = context['ti'].xcom_pull(key='keyNINE',task_ids='push9')
    print (value)
    return value

#Pull values 
Stage4 = PythonOperator(
    task_id ='pull_xcom9',
    python_callable=getNINE,
    provide_context=True,
    dag=dag
    )

def tell_slack(**context):
    webhook = BaseHook.get_connection('Slack2').password
    message = "hey there! we connected to slack"
    alterHook = SlackWebhookOperator(
        task_id = 'integrate_slack',
        http_conn_id='Slack2',
        webhook_token=webhook,
        message = message,
        username='Vaga',
        dag=dag)
    return alterHook.execute(context=context)


Stage7 = PythonOperator(
     task_id ='slack_task2',
     python_callable=tell_slack,
     provide_context=True,
     dag=dag
   )


Stage1 >> Stage2
