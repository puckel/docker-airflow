# VERSION 1.10.4
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.7-slim-stretch
LABEL maintainer="Puckel_"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.4
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Define en_US.
ENV LANGUAGE zh_CN.UTF-8
ENV LANG zh_CN.UTF-8
ENV LC_ALL zh_CN.UTF-8
ENV LC_CTYPE zh_CN.UTF-8
ENV LC_MESSAGES zh_CN.UTF-8


COPY script/sources.list  /etc/apt/sources.list

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
        netcat \
        locales \
    && sed -i 's/^# zh_CN.UTF-8 UTF-8$/zh_CN.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=zh_CN.UTF-8 LC_ALL=zh_CN.UTF-8 \
    && /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} -p "$(openssl passwd -1 airflow)" -G sudo airflow \
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

RUN mkdir /root/.pip && mkdir ${AIRFLOW_USER_HOME}/.pip
COPY script/pip.conf  /root/.pip/pip.conf
COPY script/pip.conf  ${AIRFLOW_USER_HOME}/.pip/pip.conf
RUN  pip install -i https://pypi.tuna.tsinghua.edu.cn/simple  -U pip setuptools wheel \
    && pip install -i https://pypi.tuna.tsinghua.edu.cn/simple  pytz pyOpenSSL \
    ndg-httpsclient \
    pyasn1 \
    apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
   'redis==3.2' \
    ldap3 \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install -i https://pypi.tuna.tsinghua.edu.cn/simple  ${PYTHON_DEPS}; fi \
    && rm -rf \
        /tmp/* \
        /var/tmp/* 

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
# 针对hive认证做了自定义修改
# COPY script/hive_hooks.py /usr/local/lib/python3.7/site-packages/airflow/hooks/hive_hooks.py

# server log中文乱码问题
COPY script/cli.py /usr/local/lib/python3.7/site-packages/airflow/bin/cli.py

# 中国时区
COPY script/timezone.py /usr/local/lib/python3.7/site-packages/airflow/utils/timezone.py
COPY script/sqlalchemy.py /usr/local/lib/python3.7/site-packages/airflow/utils/sqlalchemy.py
COPY script/master.html /usr/local/lib/python3.7/site-packages/airflow/www/templates/admin/master.html

RUN chown -R airflow: ${AIRFLOW_USER_HOME}

EXPOSE 8080 5555 8793
# 通过以下方式添加hive支持
# wget https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz
# wget https://mirrors.tuna.tsinghua.edu.cn/apache/hive/hive-1.2.2/apache-hive-1.2.2-bin.tar.gz
# wget jdk8u221
#ADD apache-hive-1.1.0-bin.tar.gz /opt/
#ADD hadoop-2.7.7.tar.gz /opt/
#ADD jdk8u221.tgz /opt/
#ENV JAVA_HOME=/opt/jdk1.8.0_221
#ENV HADOOP_HOME=/opt/hadoop-2.7.7
#ENV HIVE_HOME=/opt/apache-hive-1.1.0-bin
#ENV PATH="${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HIVE_HOME}/bin:${PATH}"

RUN chmod 777 /etc/profile.d
USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
