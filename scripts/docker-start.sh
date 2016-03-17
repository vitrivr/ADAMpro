#!/bin/sh

eval "$(docker-machine env default)"

docker start postgresql
docker start cassandra
docker start spark