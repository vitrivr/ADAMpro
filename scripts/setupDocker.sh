#!/bin/bash

####################
# builds a self-contained docker ADAMpro image with all necessary packages installed and runs the container
####################

DOCKERMACHINE=adampro
ADAMPRODIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

# build containers
docker network create --driver bridge vitrvrnw
docker build -t adampro:latest $ADAMPRODIR/scripts/docker

# run container
docker run --net=vitrvrnw -d -h $DOCKERMACHINE --name $DOCKERMACHINE --net-alias $DOCKERMACHINE -p 5005:5005 -p 5890:5890 -p 9099:9099 -p 5432:5432 -p 9000:9000 -p 4040:4040 -p 10088:10088 -d adampro:latest

# output
echo "Self-contained ADAMpro container has been built and started; open http://localhost:4040"
echo "Run ´docker save -o adampro.tar adampro´ to export."