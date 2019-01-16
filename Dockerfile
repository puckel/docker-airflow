FROM python:3.6-slim

# Airflow
ARG AIRFLOW_VERSION=d35439a82b11848d25596c44563ae2ac2a1fd134
ENV AIRFLOW_HOME /usr/local/airflow
ENV SLUGIFY_USES_TEXT_UNIDECODE yes
ENV AIRFLOW_GPL_UNIDECODE yes
ENV AIRFLOW__CORE__EXECUTOR KubernetesExecutor

RUN set -ex \
    && buildDeps=' \
        python3-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        python3-requests \
        default-libmysqlclient-dev \
        curl \
        rsync \
        netcat-openbsd \
        git \
#        libstdc++6 \
    && pip install -U pip setuptools wheel cython\
    && pip install kubernetes cryptography psycopg2 flask_oauthlib scp pyarrow pandas tqdm \
    && pip install git+https://github.com/apache/incubator-airflow.git@$AIRFLOW_VERSION#egg=apache-airflow[crypto,postgres,jdbc,mysql,s3,slack,password,ssh,gcp_api,pymongo,redis] \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow
WORKDIR ${AIRFLOW_HOME}

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY dags ${AIRFLOW_HOME}/dags

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
