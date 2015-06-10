## Airflow Dockerfile


This repository contains **Dockerfile** of [airflow](https://github.com/airbnb/airflow) for [Docker](https://www.docker.com/)'s [automated build](https://registry.hub.docker.com/u/puckel/docker-airflow/) published to the public [Docker Hub Registry](https://registry.hub.docker.com/).


### Base Docker Image

* [debian:wheezy](https://registry.hub.docker.com/_/debian/)


### Installation

1. Install [Docker](https://www.docker.com/).

2. Download [automated build](https://registry.hub.docker.com/u/puckel/docker-airflow/) from public [Docker Hub Registry](https://registry.hub.docker.com/): `docker pull puckel/docker-airflow`

Alternatively, you can build an image from [Dockerfile](https://github.com/puckel/docker-airflow)

### Usage


```bash
    docker run -d \
        --name airflow \
        -p 8080:8080
        puckel/docker-airflow
```
