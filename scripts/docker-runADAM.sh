#!/bin/sh

####################
# submits the jar file to the Spark container
####################

eval "$(docker-machine env default)"

# note the way that the path is specified to the JAR: in docker-setup we have mounted the target folder to the docker VM
docker exec -i --tty=false spark spark-submit --master yarn-client --class ch.unibas.dmi.dbis.adam.main.Startup  /target/scala-2.10/ADAMpro-assembly-0.1.0.jar