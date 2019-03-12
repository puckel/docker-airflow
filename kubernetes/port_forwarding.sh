#!/bin/bash

set -x

killall kubectl
sleep 1
rm nohup.out

AIRFLOW_POD=`kubectl get pods | grep airflow-web -m 1| cut -f1 -d' '`
AIRFLOW_PORT=`kubectl get pods ${AIRFLOW_POD} --template='{{(index (index .spec.containers 0).ports 0).containerPort}}{{"\n"}}'`
nohup kubectl port-forward ${AIRFLOW_POD} ${AIRFLOW_PORT}:${AIRFLOW_PORT} &
