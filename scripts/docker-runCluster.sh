#!/usr/bin/env bash

SPARK_MASTER=10.34.58.136

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

##TODO Maybe it's better to have a submit-container on the local system?
# But the submit-container relies on spark-hadoop so scp is slicker

##Copy jar to SPARK_MASTER
scp -r $DIR/target ubuntu@$SPARK_MASTER:target/

#Launch spark-submit
ssh ubuntu@SPARK_MASTER sudo docker stop spark-submit
ssh ubuntu@SPARK_MASTER sudo docker start spark-submit