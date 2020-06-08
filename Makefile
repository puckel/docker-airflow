download-secret:
	aws secretsmanager get-secret-value --secret-id analytics/astronomer | jq -r .SecretString > creds.json

delete-secret:
	rm creds.json

replace-secret:
	docker run -v ${PWD}:/root/ hairyhenderson/gomplate \
		-d creds=file:///root/creds.json \
		-f /root/airflow_settings.tmpl \
		-o /root/airflow_settings.yaml

fill-secret: download-secret replace-secret delete-secret

run-local: fill-secret
	astro dev start

stop-local:
	astro dev stop
	rm airflow_settings.yaml

deploy: fill-secret
	astro deploy
