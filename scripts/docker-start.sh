#!/bin/bash

####################
# starts all docker container for ADAMpro
####################

eval "$(sudo docker-machine env default)"

sudo docker start postgresql
sudo docker start spark