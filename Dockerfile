FROM phusion/baseimage:latest

ARG AIRFLOW_VERSION=1.10.2
ENV AIRFLOW_HOME /usr/local/airflow
ENV SLUGIFY_USES_TEXT_UNIDECODE yes
ENV AIRFLOW_GPL_UNIDECODE yes
ENV AIRFLOW__CORE__EXECUTOR KubernetesExecutor
ENV PYTHONPATH /usr/local/airflow

RUN add-apt-repository ppa:jonathonf/python-3.6


RUN apt-get update && apt-get install --yes \
	    build-essential \
	    g++ \
	    python3.6 \
	    python3.6-dev \
    && ln -s /usr/bin/python3.6 /usr/bin/python



# install deps
RUN apt-get update -y && apt-get install -y \
        wget \
        python3.6 \
        python3.6-dev \
        python-pip \
        libczmq-dev \
        libcurlpp-dev \
        curl \
        libssl-dev \
        git \
        inetutils-telnet \
        bind9utils \
        zip \
        unzip \
    && apt-get clean

RUN curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py

RUN python3.6 /tmp/get-pip.py

RUN rm /tmp/get-pip.py

RUN pip install --upgrade pip

# Since we install vanilla Airflow, we also want to have support for Postgres and Kubernetes
RUN pip install -U setuptools
RUN pip install kubernetes
RUN pip install cryptography
RUN pip install psycopg2-binary==2.7.4  # I had issues with older versions of psycopg2, just a warning
RUN pip install scp
RUN pip install pandas==0.23.4
RUN pip install great_expectations==0.4.5
RUN pip install boto3==1.9.4
RUN pip install git+https://github.com/apache/incubator-airflow.git@$AIRFLOW_VERSION#egg=apache-airflow[crypto,postgres,jdbc,mysql,s3,slack,password,ssh,redis]

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
