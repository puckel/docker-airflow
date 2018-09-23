#!/usr/bin/env bash
set -ex
source ./PROJECT_CONFIG
REV=$(git rev-parse --short HEAD)

docker tag $DOCKER_USER_ID/$COMPONENT_NAME:latest $DOCKER_USER_ID/$COMPONENT_NAME:$REV

docker push $DOCKER_USER_ID/$COMPONENT_NAME:$REV
docker push $DOCKER_USER_ID/$COMPONENT_NAME:latest
