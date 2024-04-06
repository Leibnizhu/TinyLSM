#!/bin/bash
SCRIPT_DIR=$(cd $(dirname $0);pwd)
cd $SCRIPT_DIR/..

export LOG_LEVEL="ERROR"
sbt clean
sbt "jmh:run -i 3 -wi 3 -f1 -t1 io.github.leibnizhu.tinylsm.benchmark.*"