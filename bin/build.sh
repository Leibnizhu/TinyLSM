#!/bin/bash
SCRIPT_DIR=$(cd $(dirname $0);pwd)
cd $SCRIPT_DIR/..

sbt assembly
version=$(sbt "print version"|tail -1|sed "s/\-SNAPSHOT//")
docker build . -f Dockerfile -t leibniz007/tinylsm:$version --network=host --target prod
docker tag leibniz007/tinylsm:$version leibniz007/tinylsm:latest