#!/bin/sh

eval "$(docker-machine env default)"

docker exec -i --tty=false spark spark-submit --master yarn-client --class ch.unibas.dmi.dbis.adam.main.Startup  /target/scala-2.10/ADAMpro-assembly-0.1.0.jar