read -p 'Please enter AWS KEY: ' KEY
read -ps 'Please enter AWS SECRET: ' SECRET
airflow connections -a --conn_id "aws_credentials" --conn_type "Amazon Web Services" --conn_login $KEY --conn_password $SECRET
airflow variables -s "s3_bucket" "uk-hydrology-data-engineering"
airflow unpause Hydrology-Data-Project