#!/bin/bash
set -euo pipefail

if [[ ! -f ${HOME}/.ssh/id_rsa ]]; then
  printf "%b\n" "$AIRFLOW_GITHUB_RSA" > ${HOME}/.ssh/id_rsa
  chmod 600 ${HOME}/.ssh/id_rsa
fi

git-sync.py \
  --repo git@github.com:WeConnect/data-workflows.git \
  --dest /usr/local/airflow/repo \
  --branch pull-test \
  --run-once

if [[ ! -L ${HOME}/dags/dags ]]; then
  ln -sf \
    ${AIRFLOW__CORE__AIRFLOW_HOME}/repo/src/projects/example_project/dags \
    ${AIRFLOW__CORE__AIRFLOW_HOME}/dags/
fi
