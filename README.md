# docker-airflow 2
[![Docker Image CI](https://github.com/dataops-sre/docker-airflow2/actions/workflows/ci.yml/badge.svg)](https://github.com/dataops-sre/docker-airflow2/actions/workflows/ci.yml)

[![Docker Hub](https://img.shields.io/badge/docker-ready-blue.svg)](https://hub.docker.com/r/dataopssre/docker-airflow2)
[![Docker Pulls](https://img.shields.io/docker/pulls/dataopssre/docker-airflow2.svg)](https://hub.docker.com/r/dataopssre/docker-airflow2)
[![Docker Stars](https://img.shields.io/docker/stars/dataopssre/docker-airflow2.svg)](https://hub.docker.com/r/dataopssre/docker-airflow2)

This repository contains **Dockerfile** of [apache-airflow2](https://github.com/apache/airflow) for [Docker](https://www.docker.com/)'s [automated build](https://hub.docker.com/r/dataopssre/docker-airflow2/) published to the public [Docker Hub Registry](https://hub.docker.com/r/dataopssre/docker-airflow2).

## TL,TR
Use Docker image [dataopssre/docker-airflow2](https://hub.docker.com/r/dataopssre/docker-airflow2) to update your airflow setup

Helm chart to deploy airflow2 docker image:
```
helm repo add dataops-sre-airflow https://dataops-sre.github.io/docker-airflow2/
helm repo update
helm install airflow dataops-sre-airflow/airflow --wait --timeout 300s
```

## Informations

* Based on official Airflow 2 Image [apache/airflow2:2.1.2-python3.8
](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue
* Docker entrypoint script is forked from [puckel/docker-airflow](https://github.com/puckel/docker-airflow)
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)


## Motivation
This repo is forked form [puckel/docker-airflow](https://github.com/puckel/docker-airflow), the original repo seems not maintained.

Airflow is been updated to version 2 and release its [official docker image](https://hub.docker.com/r/apache/airflow), you can also find [bitnami airflow image](https://hub.docker.com/r/bitnami/airflow). Nevertheless, puckel's image is still interesting, in the market none of providers offer an Airflow run with LocalExecutor with scheduler in one container, it is extremely usefull when to deploy a simple Airflow to an AWS EKS cluster. With Kubernetes you can resolve Airflow scablity issue by using uniquely KubernetesPodOpetertor in your dags, then we need zero computational power for airflow, it serves pure purpose of scheduler, seperate scheduler and webserver into two different pods is a bit problematic on AWS EKS cluster, we want to keep dags and logs into a Persistant volume, but AWS has some limitation for EBS volume multi attach, which means webserver and scheduler pod has to be scheduled on the same EKS node, it is a bit annoying. Thus puckel's airflow startup script is usefull.

what this fork do :

* Disactive by default the login screen in Airflow 2
* Improve current script to only take into account Airflow environment variables
* Make sure docker compose files works
* Add Airflow2 deployment helm chart and release a public repository in Github

You can use the helm chart release in this repository, see [here](https://dataops-sre.github.io/docker-airflow2) to deploys airflow2 to a Kubernetes cluster.


## Build

Optionally install [Extra Airflow Packages](https://airflow.incubator.apache.org/installation.html#extra-package) and/or python dependencies at build time :

    docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" -t dataopssre/docker-airflow2 .
    docker build --rm --build-arg PYTHON_DEPS="requests" -t dataopssre/docker-airflow2 .

or combined

    docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="requests" -t dataopssre/docker-airflow2 .


## Usage

By default, docker-airflow runs Airflow with **SequentialExecutor** :

    docker run -d -p 8080:8080 puckel/docker-airflow webserver

If you want to run another executor, use the docker-compose.yml files provided in this repository.

For **LocalExecutor** :

    docker-compose -f docker-compose-LocalExecutor.yml up -d

For **CeleryExecutor** :

    docker-compose -f docker-compose-CeleryExecutor.yml up -d

NB : If you want to have DAGs example loaded (default=False), you've to set the following environment variable in docker-compose files :

`AIRFLOW__CORE__LOAD_EXAMPLES=True`


If you want to use Ad hoc query, make sure you've configured connections:
Go to Admin -> Connections and Edit "postgres_default" set this values (equivalent to values in airflow.cfg/docker-compose*.yml) :
- Host : postgres
- Schema : airflow
- Login : airflow
- Password : airflow

For encrypted connection passwords (in Local or Celery Executor), you must have the same fernet_key. By default docker-airflow generates the fernet_key at startup, you have to set an environment variable in the docker-compose (ie: docker-compose-LocalExecutor.yml) file to set the same key accross containers. To generate a fernet_key :

    docker run dataopssre/docker-airflow2 python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)"

## Configuring Airflow

It's possible to set any configuration value for Airflow from environment variables

The general rule is the environment variable should be named `AIRFLOW__<section>__<key>`, for example `AIRFLOW__CORE__SQL_ALCHEMY_CONN` sets the `sql_alchemy_conn` config option in the `[core]` section.

Check out the [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html) for more details

You can also define connections via environment variables by prefixing them with `AIRFLOW_CONN_` - for example `AIRFLOW_CONN_POSTGRES_MASTER=postgres://user:password@localhost:5432/master` for a connection called "postgres_master". The value is parsed as a URI. This will work for hooks etc, but won't show up in the "Ad-hoc Query" section unless an (empty) connection is also created in the DB

## Custom Airflow plugins

Airflow allows for custom user-created plugins which are typically found in `${AIRFLOW_HOME}/plugins` folder. Documentation on plugins can be found [here](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html)

In order to incorporate plugins into your docker container
- Create the plugins folders `plugins/` with your custom plugins.
- Mount the folder as a volume by doing either of the following:
    - Include the folder as a volume in command-line `-v $(pwd)/plugins/:/opt/airflow/plugins`
    - Use docker-compose-LocalExecutor.yml or docker-compose-CeleryExecutor.yml which contains support for adding the plugins folder as a volume

## Install custom python package

- Create a file "requirements.txt" with the desired python modules
- Mount this file as a volume `-v $(pwd)/requirements.txt:/requirements.txt` (or add it as a volume in docker-compose file)
- The entrypoint.sh script execute the pip install command (with --user option)

## UI Links

- Airflow: [localhost:8080](http://localhost:8080/)
- Flower: [localhost:5555](http://localhost:5555/)


## Scale the number of workers

Easy scaling using docker-compose:

    docker-compose -f docker-compose-CeleryExecutor.yml scale worker=5

This can be used to scale to a multi node setup using docker swarm.

## Running other airflow commands

If you want to run other airflow sub-commands, such as `list_dags` or `clear` you can do so like this:

    docker run --rm -ti dataopssre/docker-airflow2 airflow dags list

or with your docker-compose set up like this:

    docker-compose -f docker-compose-CeleryExecutor.yml run --rm webserver airflow dags list

You can also use this to run a bash shell or any other command in the same environment that airflow would be run in:

    docker run --rm -ti dataopssre/docker-airflow2 bash
    docker run --rm -ti dataopssre/docker-airflow2 ipython

# Simplified SQL database configuration using PostgreSQL

Here is a list of PostgreSQL configuration variables and their default values. They're used to compute
the `AIRFLOW__CORE__SQL_ALCHEMY_CONN` and `AIRFLOW__CELERY__RESULT_BACKEND` variables when needed for you
if you don't provide them explicitly:

| Variable            | Default value |  Role                |
|---------------------|---------------|----------------------|
| `POSTGRES_HOST`     | `postgres`    | Database server host |
| `POSTGRES_PORT`     | `5432`        | Database server port |
| `POSTGRES_USER`     | `airflow`     | Database user        |
| `POSTGRES_PASSWORD` | `airflow`     | Database password    |
| `POSTGRES_DB`       | `airflow`     | Database name        |
| `POSTGRES_EXTRAS`   | empty         | Extras parameters    |

You can also use those variables to adapt your compose file to match an existing PostgreSQL instance managed elsewhere.

Please refer to the Airflow documentation to understand the use of extras parameters, for example in order to configure
a connection that uses TLS encryption.

Here's an important thing to consider:

> When specifying the connection as URI (in AIRFLOW_CONN_* variable) you should specify it following the standard syntax of DB connections,
> where extras are passed as parameters of the URI (note that all components of the URI should be URL-encoded).

Therefore you must provide extras parameters URL-encoded, starting with a leading `?`. For example:

    POSTGRES_EXTRAS="?sslmode=verify-full&sslrootcert=%2Fetc%2Fssl%2Fcerts%2Fca-certificates.crt"

# Simplified Celery broker configuration using Redis

If the executor type is set to *CeleryExecutor* you'll need a Celery broker. Here is a list of Redis configuration variables
and their default values. They're used to compute the `AIRFLOW__CELERY__BROKER_URL` variable for you if you don't provide
it explicitly:

| Variable          | Default value | Role                           |
|-------------------|---------------|--------------------------------|
| `REDIS_PROTO`     | `redis://`    | Protocol                       |
| `REDIS_HOST`      | `redis`       | Redis server host              |
| `REDIS_PORT`      | `6379`        | Redis server port              |
| `REDIS_PASSWORD`  | empty         | If Redis is password protected |
| `REDIS_DBNUM`     | `1`           | Database number                |

You can also use those variables to adapt your compose file to match an existing Redis instance managed elsewhere.

# Wanna help?

Fork, improve and PR.
