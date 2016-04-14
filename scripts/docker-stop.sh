#!/bin/sh

####################
# stops all docker container for ADAMpro. when starting containers again, the data is still available.
####################

eval "$(docker-machine env default)"

docker stop postgresql
docker stop cassandra
docker stop spark