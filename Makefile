AIRFLOW_VERSION = 1.10.4

build:
	docker build --rm --build-arg AIRFLOW_VERSION=$(AIRFLOW_VERSION) -t natbusa/docker-airflow:$(AIRFLOW_VERSION) .

publish: build
	#docker push datalabframework-demos/teko-warehouse/pyspark-notebook:$(SPARK_VERSION)-hadoop-$(HADOOP_VERSION) .
