#!/usr/bin/env bash
set -e

source ./PROJECT_CONFIG

./build.sh

docker-compose -f docker-compose-LocalExecutor.yml up -d
