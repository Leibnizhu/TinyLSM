#!/bin/bash
SCRIPT_DIR=$(cd $(dirname $0);pwd)
cd $SCRIPT_DIR/..

image="leibniz007/tinylsm"
version=$(sbt "print version"|tail -1|sed "s/\-SNAPSHOT//")

echo "Working dir: $(pwd), image version: $version"
docker build . -f Dockerfile -t $image:$version --network=host --target prod
docker tag $image:$version $image:latest
docker history $image:$version