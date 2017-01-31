#!/bin/bash

####################
# builds a self-contained docker ADAMpro (adampro:latest) container with all necessary packages installed.
####################

ADAMPRODIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

docker network create --driver bridge adampronw
docker build -t adampro $ADAMPRODIR/scripts/docker
docker run --net=adampronw -d -h adamprovbs --name adamprovbs --net-alias adamprovbs -p 5890:5890 -p 9099:9099 -p 5432:5432 -p 9000:9000 -p 4040:4040 -d adampro

echo "Self-contained ADAMpro container has been built and started; open http://localhost:4040"
echo "Run ´docker save -o adampro.tar adampro´ to export."