#!/bin/bash

service ssh restart

# register at master
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
$SPARK_HOME/sbin/start-slave.sh $ADAMPRO_MASTER

while true; do sleep 1000; done