FROM apache/airflow:2.2.1-python3.9

LABEL maintainer="dataops-sre"

ARG AIRFLOW_VERSION=2.2.1
ARG PYTHON_VERSION=3.9

ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""

RUN pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh,kubernetes,snowflake${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    # plyvel Required by airflow, unknown why not installed as a dependency
    && pip install plyvel==1.3.0 \
    # pyarrow 5.0 required by snowflake but possibly incompatible by airflow
    && pip install pyarrow==5.0.0 \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi


COPY script/entrypoint.sh /entrypoint.sh
COPY config/webserver_config.py $AIRFLOW_HOME/
COPY dags $AIRFLOW_HOME/dags

ENTRYPOINT ["/entrypoint.sh"]