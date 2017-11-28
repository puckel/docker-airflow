# docker-airflow

This repository contains **Dockerfile** of [apache-airflow](https://github.com/apache/incubator-airflow) for [Docker](https://www.docker.com/).

## Information

* Based on Python (3.6-slim) official Image [python:3.6-slim](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)

## Installation

Clone this repo. 

In the Dockerfile, set AIRFLOW_VERSION to the git ref (commit sha, branch name, tags/foo_tag) for the version of Airflow you'd like to build. Default is 'master'.

List any additional python packages needed to run your Airflow code in requirements.txt.

## Build

        docker build --rm -t call/docker-airflow .

## Usage

By default, docker-airflow runs Airflow with **SequentialExecutor** :

        docker run -d -p 8080:8080 call/docker-airflow

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

Check [Airflow Documentation](https://pythonhosted.org/airflow/)


## Install custom python package

### This is from the original README– may no longer be necessary.

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

# Wanna help?

Fork, improve and PR. ;-)
