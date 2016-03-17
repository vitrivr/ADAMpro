#!/bin/sh

eval "$(docker-machine env default)"

docker stop postgresql
docker rm postgresql

docker stop cassandra
docker rm cassandra

docker stop spark
docker rm spark

docker network rm adampronw