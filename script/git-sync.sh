#!/bin/bash
set -exuo pipefail

git-sync.py --repo git@github.com:WeConnect/data-airflow.git --dest /usr/local/airflow/repo --branch master --run-once
