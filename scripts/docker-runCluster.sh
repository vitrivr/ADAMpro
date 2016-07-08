#!/usr/bin/env bash

export SPARK_MASTER=10.34.58.136

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

##TODO Maybe it's better to have a submit-container on the local system?
# But the submit-container relies on spark-hadoop so scp is slicker

ssh ubuntu@$SPARK_MASTEr sudo rm -r target/scala-2.10/

ssh ubuntu@$SPARK_MASTER mkdir -p target/scala-2.10/
##Copy jar to SPARK_MASTER
scp $DIR/target/scala-2.10/ADAMpro-assembly-0.1.0.jar ubuntu@$SPARK_MASTER:target/scala-2.10/ADAMpro-assembly-0.1.0.jar
#For copying inside project
# scp target/scala-2.10/ADAMpro-assembly-0.1.0.jar ubuntu@10.34.58.136:target/scala-2.10/ADAMpro-assembly-0.1.0.jar


#TODO Copy conf-folder

#Launch spark-submit
ssh ubuntu@$SPARK_MASTER sudo docker stop spark-submit
ssh ubuntu@$SPARK_MASTER sudo docker start spark-submit