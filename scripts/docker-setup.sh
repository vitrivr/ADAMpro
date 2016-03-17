#!/bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

echo $DIR

eval "$(docker-machine env default)"

docker network create --driver bridge adampronw

docker run --net=adampronw -p 5432:5432 -h postgresql --name postgresql --net-alias postgresql -d orchardup/postgresql:latest

docker run --net=adampronw -p 9042:9042 -h cassandra-container --name cassandra --net-alias cassandra -d cassandra:3.0

docker run --net=adampronw -d -p 8088:8088 -p 4040:4040 -p 5890:5890 -v $DIR/target:/target -h spark --name spark --net-alias spark sequenceiq/spark:1.6.0 -d