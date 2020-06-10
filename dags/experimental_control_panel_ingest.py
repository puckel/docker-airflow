from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import pathlib
import gspread

service_account_path = pathlib.Path(
    '../extras/analytics-google-service-account.json')

spreadsheet_id = '1oTdca4ldaEFXaye6Kxu8iPmyWU0NZTjTIgIBjZ6ddbk'


def get_control_panel_values(ts, **kwargs):
    gc = gspread.service_account(filename=service_account_path.as_posix())
    sh = gc.open_by_key(spreadsheet_id)

    ws = sh.get_worksheet(0)
    return ws.get_all_records()


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('experimental_control_panel_ingest',
         start_date=datetime(2020, 6, 10),
         max_active_runs=1,
         schedule_interval=timedelta(minutes=10),
         default_args=default_args,
         ) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    get_control_panel_task = PythonOperator(
        task_id='get_control_panel_values',
        python_callable=get_control_panel_values,
    )

    start_task >> get_control_panel_values
    pass
