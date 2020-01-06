from airflow import DAG
from datetime import timedelta, datetime

from cm_graph import create_refresh_tasks


def failure_callback(ctx):
    print("FAILED")
    # Add PD

dag = DAG("campaign_manager_refresh_tables",
          description="Recreate views and tables that CM reads from",
          schedule_interval=timedelta(minutes=30),
          concurrency=5,
          dagrun_timeout=timedelta(minutes=60),
          max_active_runs=1,
          catchup=False,
          start_date=datetime(2019, 12, 19),
          on_failure_callback=failure_callback)

refresh_tasks = create_refresh_tasks(dag)
