#!/bin/bash

if [[ $1 = "--masternode" || $2 = "--masternode" ]]; then
   echo "starting master..."

  # run ADAMpro
  $SPARK_HOME/sbin/start-master.sh
  $SPARK_HOME/bin/spark-submit --master "$ADAMPRO_MASTER" --driver-memory $ADAMPRO_DRIVER_MEMORY --executor-memory $ADAMPRO_EXECUTOR_MEMORY --deploy-mode client --driver-java-options "-Dlog4j.configuration=file:$ADAMPRO_HOME/log4j.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$ADAMPRO_HOME/log4j.properties" --class org.vitrivr.adampro.main.Startup $ADAMPRO_HOME/ADAMpro-assembly-0.1.0.jar &

  # start web UI
  java -jar -Dlog4j.configuration=file:$ADAMPRO_HOME/log4j.properties $ADAMPRO_HOME/ADAMpro-web-assembly-0.1.0.jar &
fi

if [[ $1 = "--workernode" || $2 = "--workernode" ]]; then
   echo "starting worker..."
   $SPARK_HOME/sbin/start-slave.sh $ADAMPRO_MASTER
fi

# netstat
/usr/sbin/netdata

# startup
if [[ $1 = "-bash" || $2 = "-bash" ]]; then
  /bin/bash
fi

while true
do
  tail -f /dev/null & wait ${!}
done