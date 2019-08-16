# VERSION 1.10.2
# AUTHOR: Paul Foran
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.6-slim
LABEL maintainer="PaulForan_"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.2
ARG AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_GPL_UNIDECODE yes

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8


RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        libpq5 \
        netcat \
        locales \
        vim \
        wget \
        unzip \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install boto3 \
    && pip install apache-airflow[crypto,celery,postgres,hive,kubernetes,jdbc,mysql,oracle,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && pip install 'redis==3.2.1' \
    && pip install cx-Oracle==7.2.1 \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
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
    

# Install Oracle Instantclient
RUN mkdir /opt/oracle \
    && cd /opt/oracle \
    && wget https://github.com/epoweripione/oracle-instantclient-18/raw/master/instantclient-basic-linux.x64-19.3.0.0.0dbru.zip \
    && wget https://github.com/epoweripione/oracle-instantclient-18/raw/master/instantclient-sdk-linux.x64-19.3.0.0.0dbru.zip \
    && unzip /opt/oracle/instantclient-basic-linux.x64-19.3.0.0.0dbru.zip -d /opt/oracle \
    && unzip /opt/oracle/instantclient-sdk-linux.x64-19.3.0.0.0dbru.zip -d /opt/oracle \
    #&& ln -s /opt/oracle/instantclient_19_3/libclntsh.so.19.1 /opt/oracle/instantclient_19_3/libclntsh.so \
    #&& ln -s /opt/oracle/instantclient_19_3/libclntshcore.so.19.1 /opt/oracle/instantclient_19_3/libclntshcore.so \
    #&& ln -s /opt/oracle/instantclient_19_3/libocci.so.19.1 /opt/oracle/instantclient_19_3/libocci.so \
    && rm -rf /opt/oracle/*.zip
RUN apt-get update \
    && apt-get install libaio1
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_19_3 
    
COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint