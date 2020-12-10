upload-dags:
	aws s3 sync ./dags s3://dojo-airflow-dags/data

run-local:
	docker run -it  -p 8080:8080 -v $(shell pwd)/requirements.txt:/requirements.txt -v $(shell pwd)/dags:/usr/local/airflow/dags puckel/docker-airflow bash