TAG=1.9.0-3

build-base:
	docker build --rm \
		-t we/docker-airflow:$(TAG) .

up:
	docker-compose -f docker-compose-LocalExecutor.yml up -d

down:
	

ui:
	open http://localhost:8080
