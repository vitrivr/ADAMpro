#!/usr/bin/env bash

export SPARK_MASTER=10.34.58.136

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

## Maybe it's better to have a submit-container on the local system?
# But the submit-container relies on spark-hadoop so scp is slicker
#TODO It's better because you don't have to scp everything

#TODO Some debug flag when the jar is already on spark-master

echo "ssh-ing into ubuntu"

ssh ubuntu@$SPARK_MASTER sudo rm -r /home/ubuntu/target/

ssh ubuntu@$SPARK_MASTER mkdir -p /home/ubuntu/target/scala-2.10/


scp -r $DIR/conf ubuntu@$SPARK_MASTER:target/conf/
ssh ubuntu@$SPARK_MASTER rm target/conf/application.conf

##Copy jar to SPARK_MASTER
#TODO is this necessary with deploy-mode client??
echo "scping"

scp $DIR/target/scala-2.10/ADAMpro-assembly-0.1.0.jar ubuntu@$SPARK_MASTER:target/scala-2.10/ADAMpro-assembly-0.1.0.jar

echo "scp done"
#TODO Copy conf-folder

#Launch spark-submit
ssh ubuntu@$SPARK_MASTER sudo docker stop spark-submit
ssh ubuntu@$SPARK_MASTER sudo docker start spark-submit

echo "script done"