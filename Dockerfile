FROM apache/airflow:2.1.2-python3.8

ARG AIRFLOW_VERSION=2.1.2
ARG MY_PYTHON_VERSION=3.8

COPY script/entrypoint.sh /entrypoint.sh
COPY config/webserver_config.py $AIRFLOW_HOME/
COPY dags $AIRFLOW_HOME/dags

ENTRYPOINT ["/entrypoint.sh"]