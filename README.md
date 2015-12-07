# Airflow Dockerfile
Circle CI : [![](https://circleci.com/gh/puckel/docker-airflow.svg?style=svg)](https://circleci.com/gh/puckel/docker-airflow)

ImageLayers : [![](https://badge.imagelayers.io/puckel/docker-airflow:latest.svg)](https://imagelayers.io/?images=puckel/docker-airflow:latest)

This repository contains **Dockerfile** of [airflow](https://github.com/airbnb/airflow) for [Docker](https://www.docker.com/)'s [automated build](https://registry.hub.docker.com/u/puckel/docker-airflow/) published to the public [Docker Hub Registry](https://registry.hub.docker.com/).

## Informations

* Based on Debian Wheezy official Image [debian:wheezy](https://registry.hub.docker.com/_/debian/)
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Following the Airflow release from [Python Package Index](https://pypi.python.org/pypi/airflow)

## Installation

        docker pull puckel/docker-airflow

## Build

For example, if you need to install [Extra Packages](http://pythonhosted.org/airflow/installation.html#extra-package), edit the Dockerfile and than build-it.

        docker build --rm -t puckel/docker-airflow .

# Usage

Start the stack (mysql, rabbitmq, airflow-webserver, airflow-scheduler airflow-flower & airflow-worker) :

        docker-compose up -d

If you want to use Ad hoc query, make sure you've configured connections :
Go to Admin -> Connections and Edit "mysql_default" set this values (equivalent to values in airflow.cfg/docker-compose.yml) :
- Host : mysql
- Schema : airflow
- Login : airflow
- Password : airflow

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
