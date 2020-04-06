AIRFLOW_VERSION:=1.10.2

build:
	docker build . \
		--build-arg AIRFLOW_VERSION=$(AIRFLOW_VERSION) \
		-t u110/docker-airflow:$(AIRFLOW_VERSION)

run:
	docker-compose -f docker-compose-LocalExecutor.yml up -d
