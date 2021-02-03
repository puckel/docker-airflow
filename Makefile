SHELL = /bin/bash

.PHONY: initialize-development build-docker

initialize-development:
	# Development dependencies.
	@pip install --upgrade pylint future pre-commit yamllint
	@pre-commit install

build-docker:
	docker-compose -f docker-compose-build.yml build airflow

push-docker:
	docker-compose -f docker-compose-build.yml push airflow

pull-docker:
	docker-compose -f docker-compose-build.yml pull airflow