airflow connections -a --conn_id "aws_credentials" --conn_type "Amazon Web Services" --conn_login "AKIA55OUHUOYUOHCS7W7" --conn_password "6UuGxvoFpAFoeT90lW7StavaO9Mpfy5xTdA2xqg1"
airflow variables -s "s3_bucket" "uk-hydrology-data-engineering"
airflow unpause Hydrology-Data-Project