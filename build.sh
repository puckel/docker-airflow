#!/usr/bin/env bash
set -e

# Read the config
source ./PROJECT_CONFIG

docker build -t $COMPONENT_NAME .
docker tag $COMPONENT_NAME:latest $DOCKER_USER_ID/$COMPONENT_NAME:latest
echo "Built image $COMPONENT_NAME:latest"