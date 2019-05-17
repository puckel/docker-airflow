# VERSION 1.10.2
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM puckel/docker-airflow:latest
LABEL maintainer="iarruss@ya.ru"

USER root

RUN apt-get update -yqq \
    && apt-get upgrade -yqq \
    && mkdir -p /usr/share/man/man1 /usr/share/man/man7 \
    && apt-get install -yqq --no-install-recommends \
        openssh-client \
        postgresql-client \
    && pip install \
        attrs arrow tweepy structlog \
        click xlrd psycopg2-binary xmltodict \
        SQLAlchemy-Searchable \
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

USER airflow
