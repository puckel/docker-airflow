#!/bin/sh

set -xe

# Install local teads-central
docker run --rm -i docker-registry.teads.net/curl:master -sL http://ebzpub.ebuzzing.com/files/teads-central/get.sh | sh - 

IMAGE=$(./teads-central vars image)
HASH=$(./teads-central vars hash)
VERSION=$(docker run $IMAGE:$HASH version | tail -1 | sed s/" "//g | sed s/v//g)

./teads-central docker tag-and-push --image $IMAGE --branch-tag --custom-tags $VERSION