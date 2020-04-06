AIRFLOW_VERSION:=1.10.2

build:
	docker build . \
		--build-arg AIRFLOW_VERSION=$(AIRFLOW_VERSION) \
		--build-arg AIRFLOW_DEPS=kubernetes \
		-t u110/docker-airflow:airflow-$(AIRFLOW_VERSION)

run:
	docker-compose -f docker-compose-LocalExecutor.yml up -d
