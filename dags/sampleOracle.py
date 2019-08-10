from airflow import DAG
#from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pprint import pprint
import cx_Oracle
import os
import logging

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("sampleOracle", default_args=default_args, schedule_interval= '0 0 * * *', max_active_runs=1)

#Get Oracle Details and make a connection to the Database
connDetails =BaseHook.get_connection('oracleTest')
userName= connDetails.login
password=connDetails.password
host=connDetails.host
logging.info("HostName is: " + host)
for param in os.environ.keys():
    logging.info("ENV variable '" +param+"' is: '"+os.environ[param]+"'")


def getListOfTables(**kwargs):
    connection = cx_Oracle.connect(userName,password,host)
    cursor = connection.cursor()
    sql = "select * from dba_tables where owner ='ADMIN'"
    cursor.execute(sql)
    fields = list(map(lambda field: field[0], cursor.description))
    rows_total = 0
    rows = cursor.fetchmany(100)
    while len(rows) > 0:
        rows_total = rows_total + len(rows)
        pprint (rows)
        rows = cursor.fetchmany(100)
        logging.info("Printed: %s rows", rows_total)
    cursor.close()

tGetListOFTables = PythonOperator (task_id = 'getListOfTables',  python_callable=getListOfTables, dag = dag)

tGetListOFTables
