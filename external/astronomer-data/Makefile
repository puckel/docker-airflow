download-secret:
	aws secretsmanager get-secret-value --secret-id analytics/astronomer | jq -r .SecretString > analytics_creds.json
	aws secretsmanager get-secret-value --secret-id rds-google-data-studio | jq -r .SecretString > frontend_creds.json

delete-secret:
	rm analytics_creds.json
	rm frontend_creds.json

replace-secret:
	docker run -v ${PWD}:/root/ hairyhenderson/gomplate \
		-d analytics_creds=file:///root/analytics_creds.json \
		-d frontend_creds=file:///root/frontend_creds.json \
		-f /root/airflow_settings.tmpl \
		-o /root/airflow_settings.yaml

fill-secret: download-secret replace-secret delete-secret

run-local: fill-secret
	astro dev start

stop-local:
	astro dev stop
	rm airflow_settings.yaml

restart-local: stop-local run-local

deploy:
	astro deploy blazing-nova-0116
