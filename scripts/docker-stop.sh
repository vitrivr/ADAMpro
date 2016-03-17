#!/bin/sh

eval "$(docker-machine env default)"

docker stop postgresql
docker stop cassandra
docker stop spark