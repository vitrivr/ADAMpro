#!/bin/bash

####################
# removes all docker containers for ADAMpro. when starting containers again, the data is no longer available.
####################

eval "$(sudo docker-machine env default)"

sudo docker stop postgresql
sudo docker rm postgresql

sudo docker stop spark
sudo docker rm spark

sudo docker network rm adampronw