TAG=1.9.0-3

build-base:
	docker build --rm \
		-t we/docker-airflow:$(TAG) .

rebuild: clean build-base up

up:
	docker-compose -f docker-compose.yml up -d

down:
	docker-compose -f docker-compose.yml down -v

clean: down

util:
	docker exec -ti docker-airflow_webserver_1 /bin/bash

ui:
	open http://localhost:8080

logs:
	docker-compose logs -f
