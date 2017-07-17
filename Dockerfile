# VERSION 1.8.1
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.6
MAINTAINER Puckel_

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.8.1
ARG AIRFLOW_HOME=/usr/local/airflow
ARG GIT_KEY=e3813f9b54fbc88c1627b1af2f6953dc98bbf1d3

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN set -ex \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        python3-dev \
        libkrb5-dev \
        libsasl2-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
	libssl-dev \
        python3-pip \
        python3-requests \
        apt-utils \
        curl \
	git \
        netcat \
        locales \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
RUN set -ex \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && python3 -m pip install -U pip \
    && pip -V \
RUN set -ex \
    && pip install Cython \
    && pip install pytz \
    && pip install apache-airflow[s3,celery,postgres,hive,hdfs,jdbc]==$AIRFLOW_VERSION \
    && pip install celery[redis]==3.1.17 

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

RUN git clone https://${GIT_KEY}@github.com/bellhops/priest ${AIRFLOW_HOME}/priest
RUN cp -R ${AIRFLOW_HOME}/priest/dags ${AIRFLOW_HOME}/dags
RUN set -ex \
    && pip install -r ${AIRFLOW_HOME}/priest/requirements.txt

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
ENV PYTHONPATH $PYTHONPATH:${AIRFLOW_HOME}/priest/src
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
