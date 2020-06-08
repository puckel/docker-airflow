download-secret:
	aws secretsmanager get-secret-value --secret-id analytics/astronomer | jq -r .SecretString > creds.json

delete-secret:
	rm creds.json

replace-secret:
	cat creds.json | jq .username | xargs -I {} sed -i.bak 's|__ANALYTICS_USERNAME__|{}|' airflow_settings.yaml
	cat creds.json | jq .host | xargs -I {} sed -i.bak 's|__ANALYTICS_HOST__|{}|' airflow_settings.yaml
	cat creds.json | jq .password | xargs -I {} sed -i.bak 's|__ANALYTICS_PASSWORD__|{}|' airflow_settings.yaml
	cat creds.json | jq .port | xargs -I {} sed -i.bak 's|__ANALYTICS_PORT__|{}|' airflow_settings.yaml
	rm *.bak

fill-secret: download-secret replace-secret delete-secret
