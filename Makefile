fill-secret:
	aws secretsmanager get-secret-value --secret-id analytics/astronomer | jq -r .SecretString | jq .

