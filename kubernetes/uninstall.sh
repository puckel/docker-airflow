#!/bin/bash
##############################################################
# Function to remove airflow from kubernetes cluster
##############################################################
uninstall_airflow() {

  # Remove port forwarding
  PORT_FORWARD_PROCESS=`ps -ef | grep "kubectl port-forward airflow" | grep -v "grep" | awk '{ print $2 }'`
  kill ${PORT_FORWARD_PROCESS}

  # Uninstall airflow via helm
  helm delete --purge airflow
  echo "Waiting 60 seconds for services to shut down"
  sleep 60

  # Remove pods that refuse to go away
  export AIRFLOW_TO_PURGE=`kubectl get pods | grep airflow | cut -f1 -d' '`
  for i in "${AIRFLOW_TO_PURGE[@]}"
  do
    echo "Purging: ${i}"
    kubectl delete pods $i --grace-period=0 --force
  done
  
  kubectl delete clusterrolebinding airflow 
  kubectl delete serviceaccount airflow --namespace=${NAMESPACE}
}


uninstall_airflow