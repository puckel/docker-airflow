# VERSION 1.8.1-2
# AUTHOR: Brian Call (forked from puckel/docker-airflow)
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t call/docker-airflow .
# SOURCE: https://github.com/call/docker-airflow

FROM python:3.6-slim
LABEL maintainer call
ENV TERM linux

# Airflow git ref, e.g. 'master', 'tags/1.0.1'
ARG AIRFLOW_GIT_REF=master
ARG AIRFLOW_HOME=/usr/local/airflow

# Set Locale for Debian
# Tip to https://github.com/cpuguy83/docker-debian/blob/master/Dockerfile
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update -qq \
    && apt-get install -y -qq locales \
    && locale-gen en_US.UTF-8 en_us \
    && dpkg-reconfigure locales \
    && locale-gen C.UTF-8 \
    && /usr/sbin/update-locale LANG=C.UTF-8

ENV LANG C.UTF-8
ENV LANGUAGE C.UTF-8
ENV LC_ALL C.UTF-8

COPY requirements.txt /tmp/requirements.txt

RUN set -ex \
    && buildDeps=' \
        python3-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -qq -y \
    && apt-get install -qq -y --no-install-recommends \
        $buildDeps \
        python3-pip \
        python3-requests \
        apt-utils \
        curl \
        netcat \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && python -m pip install -U pip setuptools wheel \
    && pip install Cython \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && git clone https://github.com/apache/incubator-airflow.git /tmp/incubator-airflow \
    && git -C /tmp/incubator-airflow/ checkout ${AIRFLOW_GIT_REF} \
    && pip install /tmp/incubator-airflow[crypto,celery,ldap,postgres] \
    && pip install celery[redis]==3.1.17 \
    && pip install -r /tmp/requirements.txt \
    && apt-get purge --auto-remove -qq -y $buildDeps \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY dags/ ${AIRFLOW_HOME}/dags/
COPY plugins/ ${AIRFLOW_HOME}/plugins/

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]

# Reset build ENV settings
ENV DEBIAN_FRONTEND teletype
