FROM python:3.8

ARG AIRFLOW_VERSION=2.1.2
ARG MY_PYTHON_VERSION=3.8
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${MY_PYTHON_VERSION}.txt"
ENV AIRFLOW_HOME="/usr/local/airflow"

RUN mkdir -p $AIRFLOW_HOME

RUN apt-get update && apt-get install -y \
    netcat

#RUN pip3 install 'apache-airflow[statsd]'
RUN pip install "apache-airflow[kubernetes,snowflake]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
RUN pip install psycopg2-binary pyarrow

COPY script/entrypoint.sh /entrypoint.sh
COPY dags $AIRFLOW_HOME/dags

ENTRYPOINT ["/entrypoint.sh"]