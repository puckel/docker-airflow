#!/bin/bash
##############################################################
# This script currently only works on OSX
#
# OSX instructions:
# Install docker and enable kubernetes:
#
# https://docs.docker.com/docker-for-mac/install/
#
# enable kubernetes:
# - Top right go Docker -> "Preferences..." -> "Kubernetes"
#   - Check mark "enable kubernetes", "Show system containers"
#   - Select "kubernetes" instead of swarm
#
# Make sure python is install along with cryptography
#
# pip3 install cryptography
#
##############################################################

NAMESPACE="default"

##############################################################
# Check everything is up and running
# Install helm if not running
##############################################################
setup () {
  # Check docker is running
  which docker
  if [ $? -eq 0 ]
  then
      echo "Docker installed"
  else
      exit 1
  fi

  # Check node is up
  [[ $(kubectl get nodes | grep Ready) ]] || exit 1

  # Install Helm
  brew list kubernetes-helm || brew install kubernetes-helm

  ##########################################
  # Install Tiller on the cluster
  ##########################################
  
  # add a service account within a namespace to segregate tiller
  kubectl --namespace kube-system create sa tiller
  
  # create a cluster role binding for tiller
  kubectl create clusterrolebinding tiller \
    --clusterrole cluster-admin \
    --serviceaccount=kube-system:tiller 
  
  helm init --service-account tiller
  echo "Waiting for tiller to come up (30 seconds)"
  sleep 30

  # Wait for Tiller to launch
  #while [ $(kubectl -n kube-system get po | grep tiller | awk '$2 == "1/1" { print $2 }') != "1/1" ]
  #do 
  #  echo "Tiller not up yet"
  #  sleep 10
  #done
}

##############################################################
# Function to install airflow via helm
##############################################################
install_airflow () {
  cd helm-chart

  # Build necessary dependencies
  helm dep build

  # Generate a fernet key
  FERNET_KEY=`python3 -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)"`

  # Setup namespace
  kubectl create namespace ${NAMESPACE}
  
  # add a service account within a namespace for airflow
  # This will allow the worker nodes to spawn pods
  kubectl --namespace ${NAMESPACE} create sa airflow
  kubectl create clusterrolebinding airflow \
    --clusterrole cluster-admin \
    --serviceaccount=${NAMESPACE}:airflow

  # Install via helm
  helm install --namespace "${NAMESPACE}" --name "airflow" --set airflow.fernet_key="$FERNET_KEY" .
  
  # Wait a few seconds
  sleep 5

  # Make sure all services are up (All airflow services return 1 before moving on)
  is_done="FALSE"
  while [ "$is_done" != "TRUE" ]
  do 
    airflow_pods_list=`kubectl get pods | grep airflow | awk '{ print $2 }'`
    is_done="TRUE"
    while read -r line; do
      if [ "$line" != "1/1" ]
      then
        echo "Services are not up yet (waiting 10 seconds)"
        is_done="FALSE"
        break
      fi
    done <<< "$airflow_pods_list"
    sleep 10
  done
  
  ##############################################################
  # Copy kubernetes config to worker pod
  ##############################################################

  # Get a copy of the kubernetes config
  cp ~/.kube/config ./custom_kube_config

  # Replace "localhost" with the actual IP address of the service on the cluster
  KUBERNETES_IP=`kubectl get services | grep kubernetes | awk '$1 == "kubernetes" { print $3 }'`
  KUBERNETES_PORT=`kubectl get services | grep kubernetes | awk '$1 == "kubernetes" { print $5 }' | cut -f1 -d'/'`
  sed -i -e "s/localhost:6443/$KUBERNETES_IP:$KUBERNETES_PORT/g" custom_kube_config

  # Copy this config file to the cluster worker pods
  export AIRFLOW_WORKER_PODS=`kubectl get pods | grep airflow-worker | cut -f1 -d' '`
  export AIRFLOW_WORKER_PODS=($AIRFLOW_WORKER_PODS)
  #for AIRFLOW_WORKER_POD in "${AIRFLOW_WORKER_PODS[@]}"
  #do
  #  echo "Copying kube config to worker pod: ${AIRFLOW_WORKER_POD}"
  #  kubectl cp custom_kube_config ${AIRFLOW_WORKER_POD}:/usr/local/airflow/.kube/config
  #done
  
  ##############################################################
  # Create Secrets on the cluster
  ##############################################################
  
  # Google credential secrets as file
  kubectl create secret generic invoice-processing-env --from-env-file=./secrets.env 
  kubectl create secret generic invoice-processing-file --from-file=./secrets.json
  
  ##############################################################
  # Test connecting to cluster
  ##############################################################
  APISERVER=$(kubectl config view --minify | grep server | cut -f 2- -d ":" | tr -d " ")
  TOKEN=$(kubectl describe secret $(kubectl get secrets | grep ^default | cut -f1 -d ' ') | grep -E '^token' | cut -f2 -d':' | tr -d " ")
  curl $APISERVER/api --header "Authorization: Bearer $TOKEN" --insecure

  ##############################################################
  # Enable access to the web GUI
  #   Helpful instructions:
  #   https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/
  ##############################################################

  # Get the name of the airflow pod
  export AIRFLOW_POD=`kubectl get pods | grep airflow-web -m 1| cut -f1 -d' '`

  # get the port
  export AIRFLOW_PORT=`kubectl get pods ${AIRFLOW_POD} --template='{{(index (index .spec.containers 0).ports 0).containerPort}}{{"\n"}}'`

  # Forward the port to the host
  nohup kubectl port-forward ${AIRFLOW_POD} ${AIRFLOW_PORT}:${AIRFLOW_PORT} &

  echo "Airflow is now up and running on: http://localhost:8080/"
}

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


setup
install_airflow
#uninstall_airflow

##############################################################
# Extras
##############################################################

#
# Enable Kubernetes web GUI
# Info: https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/
#
#kubectl create -f https://raw.githubusercontent.com/kubernetes/dashboard/master/src/deploy/recommended/kubernetes-dashboard.yaml
#kubectl proxy

# Example of how to enter a running container
# kubectl exec -it <POD_NAME> -- /bin/bash