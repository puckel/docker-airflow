#!/bin/bash

# Get Airflow web pod to set variables
export AIRFLOW_POD=`kubectl get pods | grep airflow-web -m 1| cut -f1 -d' '`


# Source env we will need
source ./airflow_vars.env


######################################
# Add variables to airflow
######################################

# Number of obs per spark file overwrite
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_model_training_manual_obs_per_file 250"

# insert docker image name
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_model_training_experiments_docker_tag 564768c11042db15371673a4705ea5c6690b5ef1"
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_model_training_manual_docker_tag 564768c11042db15371673a4705ea5c6690b5ef1"

# Insert slack oauth
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow connections --add --conn_id slack_connection_airflow --conn_type http --conn_password ${SLACK_OAUTH}"




