#!/bin/bash

# Log in to docker (requires awscli installed and authed)
$(aws ecr get-login --no-include-email)

# Get Current git hash
GIT_COMMIT=$(git log --pretty=format:'%h' -n 1)

# Build Docker image
docker build \
  --build-arg GIT_COMMIT=$GIT_COMMIT \
  -t calm/docker-airflow:$GIT_COMMIT \
  -t calm/docker-airflow:latest .

# Tag Docker Image with ECR Repo
docker tag calm/docker-airflow:$GIT_COMMIT 864879987165.dkr.ecr.us-east-1.amazonaws.com/calm/docker-airflow:$GIT_COMMIT
docker tag calm/docker-airflow:latest 864879987165.dkr.ecr.us-east-1.amazonaws.com/calm/docker-airflow:latest

# Push tagged images to registry
docker push 864879987165.dkr.ecr.us-east-1.amazonaws.com/calm/docker-airflow:$GIT_COMMIT
docker push 864879987165.dkr.ecr.us-east-1.amazonaws.com/calm/docker-airflow:latest
