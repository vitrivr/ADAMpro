#!/bin/sh

####################
# creates and sets up all necessary docker containers for ADAMpro. the containers are started.
####################

eval "$(docker-machine env default)"

docker network create --driver bridge adampronw

# creates postgresql container
docker run --net=adampronw -p 5432:5432 -h postgresql --name postgresql --net-alias postgresql -d orchardup/postgresql:latest

# creates cassandra container
docker run --net=adampronw -p 9042:9042 -h cassandra-container --name cassandra --net-alias cassandra -d cassandra:3.0

# creates Spark container
# note that we mount the target folder to the docker VM
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"
docker run --net=adampronw -d -p 8088:8088 -p 4040:4040 -p 5890:5890 -v $DIR/target:/target -h spark --name spark --net-alias spark sequenceiq/spark:1.6.0 -d