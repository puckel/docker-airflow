#!/bin/bash

# Get Airflow web pod to set variables
export AIRFLOW_POD=`kubectl get pods | grep airflow-web -m 1| cut -f1 -d' '`

# Source env we will need
source ./airflow_vars.env

######################################
# Add variables to airflow
######################################

# Experiments
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_experiments_always_train True"
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_experiments_counter 10"
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_experiments_docker_tag d0d7270622cb4c9efbe8f74f8ea740e3dfe862a0"
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_experiments_obs_per_file 100"

# Manual
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_manual_always_train True"
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_manual_counter 16"
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_manual_docker_tag d0d7270622cb4c9efbe8f74f8ea740e3dfe862a0"

# Reference
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_reference_counter 6"
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow variables --set invoice_processing_reference_docker_tag d0d7270622cb4c9efbe8f74f8ea740e3dfe862a0"

######################################
# Add connections to airflow
######################################

# Insert slack oauth
kubectl exec ${AIRFLOW_POD} -- bash -c "/entrypoint.sh airflow connections --add --conn_id slack_connection_airflow --conn_type http --conn_password ${SLACK_OAUTH}"

