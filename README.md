# Airflow Dockerfile
[![Circle CI](https://circleci.com/gh/puckel/docker-airflow.svg?style=svg)](https://circleci.com/gh/puckel/docker-airflow)

This repository contains **Dockerfile** of [airflow](https://github.com/airbnb/airflow) for [Docker](https://www.docker.com/)'s [automated build](https://registry.hub.docker.com/u/puckel/docker-airflow/) published to the public [Docker Hub Registry](https://registry.hub.docker.com/).

## Informations

* Based on Debian Wheezy official Image [debian:wheezy](https://registry.hub.docker.com/_/debian/)
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)

## Installation

        docker pull puckel/docker-airflow

## Build

For example, if you need to install [Extra Packages](http://pythonhosted.org/airflow/installation.html#extra-package), edit the Dockerfile and than build-it.

        docker build --rm -t puckel/docker-airflow .

# Usage

Start the stack (mysql, rabbitmq, airflow-webserver, airflow-scheduler airflow-flower & airflow-worker) :

        docker-compose up -d

Check [Airflow Documentation](http://pythonhosted.org/airflow/)

## UI Links

- Airflow: [localhost:8080](http://localhost:8080/)
- Flower: [localhost:5555](http://localhost:5555/)
- RabbitMQ: [localhost:15672](http://localhost:15672/)

(with boot2docker, use: open http://$(boot2docker ip):8080)


## Run the test "tutorial"

        docker exec dockerairflow_webserver_1 airflow backfill tutorial -s 2015-05-01 -e 2015-06-01

# Wanna help?

Fork, improve and PR. ;-)
