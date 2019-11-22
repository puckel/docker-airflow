.PHONY: run build

build:
	docker build --rm -t puckel/docker-airflow .

run: build
	docker run -d -p 8080:8080 puckel/docker-airflow
	@echo airflow running on http://localhost:8080

kill:
	@echo "Killing docker-airflow containers"
	docker kill $(shell docker ps -q --filter ancestor=puckel/docker-airflow)

tty:
	docker exec -i -t $(shell docker ps -q --filter ancestor=puckel/docker-airflow) /bin/bash

