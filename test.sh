#!/bin/sh
set -xe

# Install local teads-central
docker run --rm -i docker-registry.teads.net/curl:master -sL http://ebzpub.ebuzzing.com/files/teads-central/get.sh | sh -

cleanup () { trap '' INT; ./teads-central docker clean-tagged; }
trap cleanup EXIT TERM
trap true INT

# unify GNU/BSD xargs behavior
xargs -h 2>&1 | grep -q gnu && alias xargs='xargs -r'

# common changes above this line should be done upstream #
##########################################################

HASH=$(./teads-central vars hash)
IMAGE=$(./teads-central vars image)

# copy GCP credentials into work dir - they get copied during container build
cp -R -f /var/lib/jenkins/gcp-credentials .

# build and tag the image with git hash
docker build -t ${IMAGE}:${HASH} .

# Test the script can be executed
docker run --rm ${IMAGE}:${HASH} version