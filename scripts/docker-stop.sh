#!/bin/bash

####################
# stops all docker container for ADAMpro. when starting containers again, the data is still available.
####################

docker stop solr
docker stop postgresql
docker stop spark

echo "Containers for ADAMpro have been stopped."