#!/bin/sh

####################
# starts all docker container for ADAMpro
####################

eval "$(docker-machine env default)"

docker start postgresql
docker start cassandra
docker start spark