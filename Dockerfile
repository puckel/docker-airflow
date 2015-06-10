# VERSION 1.0
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

ENV AIRFLOW_HOME /usr/local/airflow

RUN apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
    python-pip \
    python-dev \
    build-essential \
    && mkdir /usr/local/airflow \
    && pip install airflow \
    && airflow initdb \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base

EXPOSE 8080

CMD ["airflow","webserver","-p","8080"]
