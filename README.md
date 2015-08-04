## Airflow Dockerfile


This repository contains **Dockerfile** of [airflow](https://github.com/airbnb/airflow) for [Docker](https://www.docker.com/)'s [automated build](https://registry.hub.docker.com/u/puckel/docker-airflow/) published to the public [Docker Hub Registry](https://registry.hub.docker.com/).


### Base Docker Image

* [debian:wheezy](https://registry.hub.docker.com/_/debian/)


### Installation

1. Install [Docker](https://www.docker.com/).

2. Install [Docker-compose](https://docs.docker.com/compose/install/).

3. Download [automated build](https://registry.hub.docker.com/u/puckel/docker-airflow/) from public [Docker Hub Registry](https://registry.hub.docker.com/): `docker pull puckel/docker-airflow`

Alternatively, you can build an image from [Dockerfile](https://github.com/puckel/docker-airflow)

### Usage

Start the stack (mysql, rabbitmq, airflow-webserver, airflow-flower & airflow-worker) :

```bash
  docker-compose up
```

UI Interface :

Airflow: http://container-ip:8080/
Flower (Celery): http://container-ip:5555/
RabbitMQ: http://container-ip:15672/

To scale the number of workers :

```bash
  docker-compose scale worker=5
```

Then you can run the "tutorial" :

```bash
  docker exec dockerairflow_webserver_1 airflow backfill tutorial -s 2015-05-01 -e 2015-06-01
```
