#!/bin/bash

####################
# removes all docker containers for ADAMpro. when starting containers again, the data is no longer available.
####################

docker stop solr
docker rm solr

docker stop postgresql
docker rm postgresql

docker stop spark
docker rm spark

docker network rm adampronw

echo "Containers have been removed."