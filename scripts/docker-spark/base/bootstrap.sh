#!/bin/bash
echo spark.yarn.jar hdfs:///spark/spark-assembly-1.6.2-hadoop2.6.0.jar > $SPARK_HOME/conf/spark-defaults.conf

CMD=${1:-"exit 0"}
if [[ "$CMD" == "-d" ]];
then
	service sshd stop
	/usr/sbin/sshd -D -d
else
	/bin/bash -c "$*"
fi