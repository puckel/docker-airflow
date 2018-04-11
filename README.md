# docker-airflow
[![CircleCI branch](https://img.shields.io/circleci/project/puckel/docker-airflow/master.svg?maxAge=2592000)](https://circleci.com/gh/puckel/docker-airflow/tree/master)
[![Docker Build Status](https://img.shields.io/docker/build/puckel/docker-airflow.svg)]()

[![Docker Hub](https://img.shields.io/badge/docker-ready-blue.svg)](https://hub.docker.com/r/puckel/docker-airflow/)
[![Docker Pulls](https://img.shields.io/docker/pulls/puckel/docker-airflow.svg)]()
[![Docker Stars](https://img.shields.io/docker/stars/puckel/docker-airflow.svg)]()

This repository contains **Dockerfile** of [apache-airflow](https://github.com/apache/incubator-airflow) for [Docker](https://www.docker.com/)'s [automated build](https://registry.hub.docker.com/u/puckel/docker-airflow/) published to the public [Docker Hub Registry](https://registry.hub.docker.com/).

## Informations

* Based on Python (3.6-slim) official Image [python:3.6-slim](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Following the Airflow release from [Python Package Index](https://pypi.python.org/pypi/apache-airflow)

/!\ If you want to use Airflow using Python 2, use TAG [1.8.1](https://github.com/puckel/docker-airflow/releases/tag/1.8.1)

## Installation

Pull the image from the Docker repository.

        docker pull puckel/docker-airflow

## Build

For example, if you need to install [Extra Packages](https://airflow.incubator.apache.org/installation.html#extra-package), edit the Dockerfile and then build it.

        docker build --rm -t puckel/docker-airflow .

## Usage

By default, docker-airflow runs Airflow with **SequentialExecutor** :

        docker run -d -p 8080:8080 puckel/docker-airflow

If you want to run another executor, use the other docker-compose.yml files provided in this repository.

For **LocalExecutor** :

        docker-compose -f docker-compose-LocalExecutor.yml up -d

For **CeleryExecutor** :

        docker-compose -f docker-compose-CeleryExecutor.yml up -d

NB : If you don't want to have DAGs example loaded (default=True), you've to set the following environment variable :

`LOAD_EX=n`

        docker run -d -p 8080:8080 -e LOAD_EX=n puckel/docker-airflow

If you want to use Ad hoc query, make sure you've configured connections:
Go to Admin -> Connections and Edit "postgres_default" set this values (equivalent to values in airflow.cfg/docker-compose*.yml) :
- Host : postgres
- Schema : airflow
- Login : airflow
- Password : airflow

For encrypted connection passwords (in Local or Celery Executor), you must have the same fernet_key. By default docker-airflow generates the fernet_key at startup, you have to set an environment variable in the docker-compose (ie: docker-compose-LocalExecutor.yml) file to set the same key accross containers. To generate a fernet_key :

        python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY"

## Configurating Airflow

It is possible to set any configuration value for Airflow from environment variables, which are used over values from the airflow.cfg. The general rule is the environment variable should be named `AIRFLOW__<section>__<key>`, for example `AIRFLOW__CORE__SQL_ALCHEMY_CONN` sets the `sql_alchemy_conn` config option in the `[core]` section.

Check out the [Airflow documentation](http://airflow.readthedocs.io/en/latest/configuration.html?highlight=__CORE__#setting-configuration-options) for more details

You can also define connections via environment variables by prefixing them with `AIRFLOW_CONN_` - for example `AIRFLOW_CONN_POSTGRES_MASTER=postgres://user:password@localhost:5432/master` for a connection called "postgres_master". The value is parsed as a URI. This will work for hooks etc, but won't show up in the "Ad-hoc Query" section unless an (empty) connection is also created in the DB

## Custom Airflow plugins

Airflow allows for custom user-created plugins which are typically found in `${AIRFLOW_HOME}/plugins` folder. Documentation on plugins can be found [here](https://airflow.apache.org/plugins.html)

In order to incorporate plugins into your docker container
- Create the plugins folders `plugins/` with your custom plugins.
- Mount the folder as a volume by doing either of the following: 
    - Include the folder as a volume in command-line `-v $(pwd)/plugins/:/usr/local/airflow/plugins`
    - Use docker-compose-LocalExecutor.yml or docker-compose-CeleryExecutor.yml which contains support for adding the plugins folder as a volume

## Install custom python package

- Create a file "requirements.txt" with the desired python modules
- Mount this file as a volume `-v $(pwd)/requirements.txt:/requirements.txt`
- The entrypoint.sh script execute the pip install command (with --user option)

## UI Links

- Airflow: [localhost:8080](http://localhost:8080/)
- Flower: [localhost:5555](http://localhost:5555/)


## Scale the number of workers

Easy scaling using docker-compose:

        docker-compose scale worker=5

This can be used to scale to a multi node setup using docker swarm.

## Running other airflow commands

If you want to run other airflow sub-commands, such as `list_dags` or `clear` you can do so like this:

        docker run --rm -ti puckel/docker-airflow airflow list_dags

or with your docker-compose set up like this:

        docker-compose -f docker-compose-CeleryExecutor.yml run --rm webserver airflow list_dags

You can also use this to run a bash shell or any other command in the same environment that airflow would be run in:

          docker run --rm -ti puckel/docker-airflow bash
          docker run --rm -ti puckel/docker-airflow ipython

# Wanna help?

Fork, improve and PR. ;-)
