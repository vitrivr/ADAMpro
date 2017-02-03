#!/bin/bash

####################
# prepare Docker containers for running unit tests
####################

ADAMPRODIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

docker network create --driver bridge vitrvrnw

# creates postgresql container
docker run --net=adampronw -p 5432:5432 -e POSTGRES_PASSWORD=adampro -e POSTGRES_DB=adampro -e POSTGRES_USER=adampro  -h postgresql --name postgresql --net-alias postgresql -d postgres:9.4

# creates a solr container
docker run --net=adampronw -p 8983:8983 -h solr --name solr --net-alias solr -d solr:6.1

# create a postgis container
docker run --net=adampronw -p 5433:5432 -e POSTGRES_PASSWORD=adampro -e POSTGRES_DB=adampro -e POSTGRES_USER=adampro  -h postgis --name postgis --net-alias postgis -d mdillon/postgis

# create a cassandra container
docker run --net=adampronw -p 9042:9042 -h cassandra --name cassandra --net-alias cassandra -d cassandra:3.7

# output
echo "Containers for running ADAMpro have been set up"