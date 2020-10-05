"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from operators.get_stations_api_operator import GetStationsAPIOperator
from operators.get_hydrology_api_operator import GetHydrologyAPIOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "max_active_runs": 1,
    "start_date": datetime(2020, 9, 30),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Hydrology-Data-Project", default_args=default_args
)

t1 = GetStationsAPIOperator(
    task_id="Get_Hydrology_Stations_from_API",
    aws_conn_id="aws_credentials",
    API_endpoint="https://environment.data.gov.uk/hydrology/id/stations.json?observedProperty={"
    "observed_property}&_limit=10",
    columns_to_drop=["easting", "northing", "notation", "type", "wiskiID", "RLOIid"],
    observed_property="waterFlow",
    target_database={
        "database": "airflow",
        "table": "stations",
        "user": "airflow",
        "password": "airflow",
    },
    dag=dag,
)

t2 = GetHydrologyAPIOperator(
    task_id="Get_Hydrology_Measures_from_API",
    source_database={
        "database": "airflow",
        "table": "stations",
        "user": "airflow",
        "password": "airflow",
    },
    target_database={
        "database": "airflow",
        "table": "measures",
        "user": "airflow",
        "password": "airflow",
    },
    provide_context=True,
    observed_property="waterFlow",
    columns_to_drop=["measure", "quality"],
    general_API_endpoint="https://environment.data.gov.uk/hydrology/data/readings.json"
    "?period={period}&station.stationReference={"
    "station_reference}&date={date}",
    date="{{ds}}",
    dag=dag,
)

t30 = GetStationsAPIOperator(
    task_id="Get_Rainfall_Stations_from_API",
    aws_conn_id="aws_credentials",
    API_endpoint="https://environment.data.gov.uk/flood-monitoring/id/stations?parameter={observed_property}&_limit=5",
    columns_to_drop=[
        "easting",
        "northing",
        "notation",
        "wiskiID",
        "RLOIid",
        "town",
        "status",
        "catchmentName",
        "dateOpened",
        "stageScale",
        "datumOffset",
        "gridReference",
    ],
    observed_property="rainfall",
    target_database={
        "database": "airflow",
        "table": "stations",
        "user": "airflow",
        "password": "airflow",
    },
    dag=dag,
)

t31 = GetStationsAPIOperator(
    task_id="Get_Rainfall_Stations_from_API",
    aws_conn_id="aws_credentials",
    API_endpoint="https://environment.data.gov.uk/flood-monitoring/id/stations?parameter={observed_property}&_limit=5",
    columns_to_drop=[
        "easting",
        "northing",
        "notation",
        "wiskiID",
        "RLOIid",
        "town",
        "status",
        "catchmentName",
        "dateOpened",
        "stageScale",
        "datumOffset",
        "gridReference",
    ],
    observed_property="level",
    target_database={
        "database": "airflow",
        "table": "stations",
        "user": "airflow",
        "password": "airflow",
    },
    dag=dag,
)

t4 = GetHydrologyAPIOperator(
    task_id="Get_Rainfall_Measures_from_API",
    source_database={
        "database": "airflow",
        "table": "stations",
        "user": "airflow",
        "password": "airflow",
    },
    target_database={
        "database": "airflow",
        "table": "measures",
        "user": "airflow",
        "password": "airflow",
    },
    provide_context=True,
    observed_property="rainfall",
    columns_to_drop=["@id", "measure"],
    general_API_endpoint="https://environment.data.gov.uk/flood-monitoring/id/stations/{"
    "station_reference}/readings.json?parameter={observed_property}&",
    date="{{ds}}",
    dag=dag,
)

t5 = GetHydrologyAPIOperator(
    task_id="Get_Level_Measures_from_API",
    source_database={
        "database": "airflow",
        "table": "stations",
        "user": "airflow",
        "password": "airflow",
    },
    target_database={
        "database": "airflow",
        "table": "measures",
        "user": "airflow",
        "password": "airflow",
    },
    provide_context=True,
    observed_property="level",
    columns_to_drop=["@id", "measure"],
    general_API_endpoint="https://environment.data.gov.uk/flood-monitoring/id/stations/{"
    "station_reference}/readings.json?parameter={observed_property}&",
    date="{{ds}}",
    dag=dag,
)

t2.set_upstream(t1)
t30.set_upstream(t1)
t31.set_upstream(t1)
t4.set_upstream(t30)
t4.set_upstream(t2)
t5.set_upstream(t31)
t5.set_upstream(t2)
