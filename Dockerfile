# VERSION 1.7.1.3-5
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

#FROM ubuntu:16.04
FROM nvidia/cuda:8.0-cudnn5-devel-ubuntu16.04

MAINTAINER Puckel_

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.8.1
ENV AIRFLOW_HOME /usr/local/airflow

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL  en_US.UTF-8
ENV PYTHONPATH=:/usr/local/airflow/dags:/usr/local/airflow/config

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
        wget \
        unzip \
        python3-pip \
        python3-dev \
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
       ubuntu-xenial \
       main" \
    && sudo apt-get update \
    && sudo apt-get -y --allow-unauthenticated install docker-engine nvidia-modprobe \
    && wget -P /tmp https://github.com/NVIDIA/nvidia-docker/releases/download/v1.0.1/nvidia-docker_1.0.1-1_amd64.deb \
    && sudo dpkg -i /tmp/nvidia-docker*.deb \
    && pip3 install --upgrade pip \
    && pip3 install setuptools \
    && pip3 install packaging \
    && pip3 install appdirs \
    && pip3 install six==1.10 \
    && pip3 install wheel \
    && pip3 install Cython \
    && pip3 install pytz==2015.7 \
    && pip3 install cryptography \
    && pip3 install pyOpenSSL \
    && pip3 install ndg-httpsclient \
    && pip3 install pyasn1 \
    && pip3 install psycopg2 \
    && pip3 install pandas==0.18.1 \
    && pip3 install celery==4.1.0 \
    && pip3 install flower>=0.7.3 \
    && pip3 install kubernetes \
    && pip3 install https://github.com/docker/docker-py/archive/1.10.6.zip \
    # && pip3 install airflow[celery,postgres,gcp_api] \
    && pip3 install https://github.com/apache/incubator-airflow/archive/v1-9-stable.zip \
    && pip3 install httplib2 \
    && pip3 install "google-api-python-client>=1.5.0,<1.6.0" \
    && pip3 install "oauth2client>=2.0.2,<2.1.0" \
    && pip3 install pandas-gbq \
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
