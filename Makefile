down :
	docker-compose -f docker-compose.yml down

up: 
	docker-compose -f docker-compose.yml up -d

build:
	docker build -t puckel/docker-airflow:1.10.4 .

util:
	docker exec -ti airflow /bin/bash

logs:
	docker-compose logs -f webserver
