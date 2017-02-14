#!/bin/bash

####################
# replace jar in existing Docker container
####################

DOCKERMACHINE=adampro
ADAMPRODIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

cd $ADAMPRODIR
sbt proto
sbt assembly
sbt web/assembly

docker cp $ADAMPRODIR/target/scala-2.11/ADAMpro-assembly-0.1.0.jar $DOCKERMACHINE:/adampro/
docker cp $ADAMPRODIR/web/target/scala-2.11/ADAMpro-web-assembly-0.1.0.jar $DOCKERMACHINE:/adampro/

docker restart $DOCKERMACHINE