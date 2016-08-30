#!/bin/bash

####################
# creates and sets up all necessary docker containers for ADAMpro. the containers are started.
####################

ADAMPRODIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

docker network create --driver bridge adampronw

# creates postgresql container
docker run --net=adampronw -p 5432:5432 -e POSTGRES_PASSWORD=adampro -e POSTGRES_DB=adampro -e POSTGRES_USER=adampro  -h postgresql --name postgresql --net-alias postgresql -d postgres:9.4

# creates a solr container
docker run --net=adampronw -p 8983:8983 -h solr --name solr --net-alias solr -d solr:6.1

# creates Spark container
mkdir -p $ADAMPRODIR/target # note that we mount the target folder to the docker VM

if [[ "$(docker images -q spark:j1.8-s1.6.1-h2.6> /dev/null)" == "" ]]; then
  docker build -t sparkj1.8 $ADAMPRODIR/scripts/docker-sparkonly
fi

cp -R $ADAMPRODIR/conf $ADAMPRODIR/target/
rm $ADAMPRODIR/target/conf/application.conf
docker run -d -e "SPARK_CONF_ADAMPRODIR=/target/conf" --net=adampronw -p 4040:4040 -p 8088:8088 -p 8042:8042 -p 5890:5890 -v $ADAMPRODIR/target:/target -h spark --name spark --net-alias spark sparkj1.8 -d

echo "Containers for ADAMpro have been set up."
echo "Run ´sbt startDocker´ to start the containers, then ´sbt runDocker´ for running the application in the containers."