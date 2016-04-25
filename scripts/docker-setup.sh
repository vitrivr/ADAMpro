#!/bin/sh

####################
# creates and sets up all necessary docker containers for ADAMpro. the containers are started.
####################

eval "$(docker-machine env default)"

docker network create --driver bridge adampronw

# creates postgresql container
docker run --net=adampronw -p 5432:5432 -h postgresql --name postgresql --net-alias postgresql -d orchardup/postgresql:latest

# creates Spark container
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"
if [[ "$(docker images -q sparkj1.8 2> /dev/null)" == "" ]]; then
  docker build -t sparkj1.8 $DIR/scripts/docker
fi

# note that we mount the target folder to the docker VM
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"
docker run --net=adampronw -d -p 8088:8088 -p 8042:8042 -p 4040:4040 -p 5890:5890 -v $DIR/target:/target -h spark --name spark --net-alias spark sparkj1.8 -d