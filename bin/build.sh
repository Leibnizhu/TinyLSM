#!/bin/bash
SCRIPT_DIR=$(cd $(dirname $0);pwd)
cd $SCRIPT_DIR/..

sbt assembly
docker build . -f Dockerfile -t ${DOCKER_IMAGE_TAG:-tiny-lsm:0.0.1} --network=host