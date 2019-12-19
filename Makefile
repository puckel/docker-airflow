upload-dags:
	aws s3 sync ./dags s3://dojo-airflow-dags
