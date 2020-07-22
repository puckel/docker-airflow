# Custom Dockerfile
FROM apache/airflow:1.10.11

# Install mssql support & dag dependencies
USER root

COPY script/entrypoint_wrapper.sh /entrypoint_wrapper.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

#EXPOSE 5555

USER airflow
ENTRYPOINT ["/entrypoint_wrapper.sh"]
