#!/bin/bash

####################
# stops all docker container for ADAMpro. when starting containers again, the data is still available.
####################

eval "$(docker-machine env default)"

sudo docker stop postgresql
sudo docker stop spark