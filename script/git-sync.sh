#!/bin/bash
set -exuo pipefail

git-sync.py --repo git@github.com:WeConnect/data-airflow.git --dest /usr/local/airflow/repo --branch pull-test --run-once

ln -sf ${AIRFLOW__CORE__AIRFLOW_HOME}/repo/src/projects/example_project/dags ${AIRFLOW__CORE__AIRFLOW_HOME}/dags/
