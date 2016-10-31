#!/usr/bin/env bash

##
# This script copies the application jar to the spark-submit container and then runs a spark-submit job on it
# It also copies config-files etc.
##
export SPARK_MASTER=10.34.58.136

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"
echo "starting bash-script"

#Stop any existing spark-submit job
ssh ubuntu@$SPARK_MASTER sudo docker stop spark-submit

#Cleanup of old jar and old conf-files
ssh ubuntu@$SPARK_MASTER sudo rm -r /home/ubuntu/target/scala-2.10/
ssh ubuntu@$SPARK_MASTER sudo rm -r /home/ubuntu/target/conf/
ssh ubuntu@$SPARK_MASTER sudo mkdir -p /home/ubuntu/target/scala-2.10/
ssh ubuntu@$SPARK_MASTER sudo chown -R ubuntu target/

#Copy conf to host
cp -R $DIR/conf $DIR/target/
rm $DIR/target/conf/application.conf
scp -r $DIR/target/conf ubuntu@$SPARK_MASTER:target/conf/


##Copy jar to SPARK_MASTER
echo "scping"
scp $DIR/target/scala-2.10/ADAMpro-assembly-0.1.0.jar ubuntu@$SPARK_MASTER:target/scala-2.10/ADAMpro-assembly-0.1.0.jar
echo "scp done"

#Currently the spark-submit job has a local file. Maybe putting the jar to hdfs or even distributing it to the workers makes more sense
#This would require changing spark-submit to deploy-mode cluster
#Another consideration would be to store the file on hdfs. Currently the link is to a file:// instead of a hdfs:// -> hadoop put first

#Launch spark-submit. Stop existing container first just to be sure
ssh ubuntu@$SPARK_MASTER sudo docker stop spark-submit
ssh ubuntu@$SPARK_MASTER sudo docker start spark-submit

echo "script done"