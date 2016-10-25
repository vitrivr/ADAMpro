#!/bin/bash

####################
# starts all docker container for ADAMpro
####################

docker start solr
docker start postgresql
docker start spark

echo "Containers for ADAMpro have been started."
echo "Run ´sbt runDocker´ for running the application in the containers."