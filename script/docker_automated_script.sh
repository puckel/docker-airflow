read -p 'Please enter AWS KEY: ' KEY
echo "OK"
read -p 'Please enter AWS SECRET: ' SECRET
echo "OK"
airflow connections -a --conn_id "aws_credentials" --conn_type "Amazon Web Services" --conn_login $KEY --conn_password $SECRET
airflow variables -s "s3_bucket" "uk-hydrology-data-engineering"
airflow unpause Hydrology-Data-Project