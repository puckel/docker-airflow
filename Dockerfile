# VERSION 1.7.1.3-5
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM ubuntu:16.04
MAINTAINER Puckel_

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.7.1.3
ENV AIRFLOW_HOME /usr/local/airflow

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL  en_US.UTF-8

RUN set -ex \
    && buildDeps=' \
        python-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
    ' \
    && echo "deb http://http.debian.net/debian jessie-backports main" >/etc/apt/sources.list.d/backports.list \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends --allow-unauthenticated \
        $buildDeps \
        python-pip \
        apt-utils \
        curl \
        netcat \
        locales \
        sudo \
        software-properties-common \
        apt-transport-https \
        ca-certificates \
        vim \
    && apt-get install -yqq --allow-unauthenticated  -t jessie-backports python-requests libpq-dev \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    # && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && echo "airflow:airflow" | chpasswd && sudo adduser airflow sudo \
    && sudo echo 'airflow  ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers \
    && curl -fsSL https://yum.dockerproject.org/gpg | sudo apt-key add - \
    && apt-key fingerprint 58118E89F3A912897C070ADBF76221572C52609D \
    && sudo add-apt-repository \
       "deb https://apt.dockerproject.org/repo/ \
       ubuntu-$(lsb_release -cs) \
       main" \
    && sudo apt-get update \
    && sudo apt-get -y install docker-engine \
    && pip install packaging \
    && pip install appdirs \
    && pip install six==1.10 \
    && pip install Cython \
    && pip install pytz==2015.7 \
    && pip install cryptography \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install psycopg2 \
    && pip install pandas==0.18.1 \
    && pip install celery==3.1.23 \
    && pip install airflow[celery,postgres,hive,hdfs,jdbc]==$AIRFLOW_VERSION \
    && pip install https://github.com/apache/incubator-airflow/archive/master.zip \
    && apt-get remove --purge -yqq $buildDeps libpq-dev \
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

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
