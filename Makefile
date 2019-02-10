all:
	@echo "make build -- build docker images"
	@echo "make run -- run container"
	@echo "make stop -- stop and remove all containers"
	@echo "make tty -- launch shell from worker container"


build:
	docker build --rm -t enrica/docker-airflow .

run:
	# CeleryExecutor
	docker-compose -f docker-compose-CeleryExecutor.yml up -d
	@echo airflow running on http://localhost:8080 if Windows http://192.168.99.100:8080

stop:
	docker stop $(docker ps -aq)

tty:
	docker exec -it $(docker-compose -f docker-compose-CeleryExecutor.yml ps -q worker) bash
