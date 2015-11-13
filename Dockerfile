# VERSION 1.1
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow
# SOURCE: https://github.com/puckel/docker-airflow

FROM debian:wheezy
MAINTAINER Puckel_

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux
# Work around initramfs-tools running on kernel 'upgrade': <http://bugs.debian.org/cgi-bin/bugreport.cgi?bug=594189>
ENV INITRD No
ENV AIRFLOW_VERSION 1.6.0
ENV AIRFLOW_HOME /usr/local/airflow
ENV PYTHONLIBPATH /usr/lib/python2.7/dist-packages

# Add airflow user
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

RUN apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
    netcat \
    curl \
    python-pip \
    python-dev \
    libmysqlclient-dev \
    libkrb5-dev \
    libsasl2-dev \
    libssl-dev \
    libffi-dev \
    build-essential \
    && pip install --install-option="--install-purelib=$PYTHONLIBPATH" cryptography \
    && pip install --install-option="--install-purelib=$PYTHONLIBPATH" airflow==${AIRFLOW_VERSION} \
    && pip install --install-option="--install-purelib=$PYTHONLIBPATH" airflow[celery]==${AIRFLOW_VERSION} \
    && pip install --install-option="--install-purelib=$PYTHONLIBPATH" airflow[mysql]==${AIRFLOW_VERSION} \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

ADD script/entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
ADD config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

RUN \
    chown -R airflow: ${AIRFLOW_HOME} \
    && chmod +x ${AIRFLOW_HOME}/entrypoint.sh

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["./entrypoint.sh"]
