SHELL = /bin/bash

.PHONY: initialize-development

build-docker:
	docker-compose -f docker-compose-build.yml build airflow