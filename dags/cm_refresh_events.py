from airflow import DAG
from datetime import timedelta, datetime

from cm_graph.create_views import create_view_tasks


def failure_callback(ctx):
    print("FAILED")
    # Add PD

dag = DAG("campaign_manager_refresh_events",
          description="Recreate views and events that CM reads from",
          schedule_interval=timedelta(minutes=30),
          concurrency=5,
          dagrun_timeout=timedelta(minutes=60),
          max_active_runs=1,
          catchup=False,
          start_date=datetime(2019, 12, 19),
          on_failure_callback=failure_callback)

create_view_tasks = create_view_tasks(dag)
