#!/bin/bash

export PODS_TO_DELETE=`kubectl get pods | grep Completed | awk '{print $1}'`
for i in "${PODS_TO_DELETE[@]}"
do
  kubectl delete pods $i
done
