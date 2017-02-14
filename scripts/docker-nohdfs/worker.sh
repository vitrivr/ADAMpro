#!/bin/bash

# register at master
$SPARK_HOME/sbin/start-slave.sh $ADAMPRO_MASTER

while true; do sleep 1000; done

