"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from operators.load_stations_operator import LoadStationsOperator
from operators.stage_stations_api_operator import StageStationsAPIOperator
from operators.get_hydrology_api_operator import GetHydrologyAPIOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "max_active_runs": 1,
    "start_date": datetime(2020, 10, 4),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Hydrology-Data-Project", default_args=default_args
)

t10 = StageStationsAPIOperator(
    task_id="Get_Hydrology_Stations_from_API",
    API_endpoint="https://environment.data.gov.uk/hydrology/id/stations.json?observedProperty={"
    "observed_property}&_limit=200",
    columns_to_drop=["easting", "northing", "notation", "type", "wiskiID", "RLOIid", "measures"],
    observed_property="waterFlow",
    target_database={
        "database": "airflow",
        "table": "stage_stations",
        "user": "airflow",
        "password": "airflow",
    },
    dag=dag,
)

t11 = StageStationsAPIOperator(
    task_id="Get_Rainfall_Stations_from_API",
    API_endpoint="https://environment.data.gov.uk/flood-monitoring/id/stations?parameter={observed_property}&_limit=200",
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
        "measures"
    ],
    observed_property="rainfall",
    target_database={
        "database": "airflow",
        "table": "stage_stations",
        "user": "airflow",
        "password": "airflow",
    },
    dag=dag,
)

t12 = StageStationsAPIOperator(
    task_id="Get_Level_Stations_from_API",
    API_endpoint="https://environment.data.gov.uk/flood-monitoring/id/stations?parameter={observed_property}&_limit=200",
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
        "town",
        "dateOpened",
        "measures",
        "downstageScale"
    ],
    observed_property="level",
    target_database={
        "database": "airflow",
        "table": "stage_stations",
        "user": "airflow",
        "password": "airflow",
    },
    dag=dag,
)

t2 = LoadStationsOperator(
    task_id="Load_Stations_from_Staging_Table",
    aws_conn_id = "aws_credentials",
    source_database={
        "database": "airflow",
        "table": "stage_stations",
        "user": "airflow",
        "password": "airflow",
    },
    target_database={
        "database": "airflow",
        "table": "stations",
        "user": "airflow",
        "password": "airflow",
    },
    dag=dag,
)

t30 = GetHydrologyAPIOperator(
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

t31 = GetHydrologyAPIOperator(
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

t32 = GetHydrologyAPIOperator(
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

t10.set_downstream(t2)
t11.set_upstream(t10)
t12.set_upstream(t10)
t11.set_downstream(t2)
t12.set_downstream(t2)
t30.set_upstream(t2)
t31.set_upstream(t2)
t32.set_upstream(t2)
