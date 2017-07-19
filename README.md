# docker-airflow

This repository contains **Dockerfile** of [apache-airflow](https://github.com/apache/incubator-airflow) for [Docker](https://www.docker.com/) for Bellhops team.

## Information

* Based on Debian Python 3.6 official Image [python:3.6](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Following the Airflow release from [Python Package Index](https://pypi.python.org/pypi/apache-airflow)

## Configuration

* Clone this repository.
* Generate your personal git token with [git-personal-token](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/#creating-a-token).
* Confgure your local docker environment file by copying [sample](./sample_env.env) to `.env` file. This environment file should be present where you are running your `docker-compose` commands.
* Change `GIT_KEY` to your git token and `PRIEST_GIT_URL` to your personal PRIEST repository like `github.com/nave91/priest`. You can also specify branch name with `PRIEST_GIT_BRANCH`. 
By default we clone `master` branch of `github.com/bellhops/priest`.

## Installation

* `cd <git_cloned_location_of_this_repo>` 
* Verify you have `.env` file with `ls -al`. You should see something similar to:
```
> docker-airflow git:(env-variables) ✗ ls -al
drwxr-xr-x  19 naveen  staff   646 Jul 19 12:14 .
drwxr-xr-x  21 naveen  staff   714 Jul 13 14:20 ..
-rw-r--r--   1 naveen  staff     5 Jul  7 11:25 .dockerignore
-rw-r--r--   1 naveen  staff   453 Jul 18 14:28 .env
```
* Run `docker-compose -f docker-compose-Postgres.yml -d up`
* Run `docker-compose -f docker-compose-CeleryExecutor -d up`
* Note: `-d` starts in detached mode and its optional.
* All standard docker-compose commands should work now.

## Priest Development
_Currently we have to kill all docker airflow containers and rebuild them to get most up-to-date priest code._
TODO: It slows down development, fix above.

#### Handy commands:
* `docker stop $(docker ps --filter "name=airflow" -q)`
* `docker rm $(docker ps --filter "name=airflow" -q)`
* `docker rmi $(docker images dockerairflow_webserver -q)`


## Scale the number of workers

Easy scaling using docker-compose:

        docker-compose scale worker=5

# Fernet Key
For encrypted connection passwords (in Local or Celery Executor), you must have the same fernet_key. By default docker-airflow generates the fernet_key at startup, you have to set an environment variable in the docker-compose (ie: docker-compose-LocalExecutor.yml) file to set the same key accross containers. To generate a fernet_key :

      python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY"

Check [Airflow Documentation](https://pythonhosted.org/airflow/)


## Install custom python package
 
- By default all packages mentioned in priest's repository `requirements.txt` are installed.

## UI Links

- Airflow: [localhost:8080](http://localhost:8080/)
- Flower: [localhost:5555](http://localhost:5555/)


