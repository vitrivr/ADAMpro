#!/bin/bash

# configuration
sed s/MASTER_HOSTNAME/$MASTER_HOSTNAME/ ${ADAM_HOME}/adampro.conf.template > $ADAM_HOME/adampro.conf

if [[ $1 = "--masternode" || $2 = "--masternode" ]]; then
   echo "starting master..."

  # storage engines
  service postgresql stop
  su --login - postgres --command "$POSTGRES_HOME/bin/pg_ctl -w start -D $PGDATA"

  # run ADAMpro
  $SPARK_HOME/sbin/start-master.sh
  $SPARK_HOME/bin/spark-submit --master "$ADAMPRO_MASTER" --driver-memory $SPARK_DRIVER_MEMORY --deploy-mode client --driver-java-options "-Dlog4j.configuration=file:$ADAM_HOME/log4j.xml" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$ADAM_HOME/log4j.xml" --driver-java-options "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder" --conf "spark.executor.extraJavaOptions=-XX:+UnlockCommercialFeatures -XX:+FlightRecorder" --class org.vitrivr.adampro.main.Startup $ADAM_HOME/ADAMpro-assembly-0.1.0.jar &

  # start web UI
  java -jar $ADAM_HOME/ADAMpro-web-assembly-0.1.0.jar &
fi

if [[ $1 = "--workernode" || $2 = "--workernode" ]]; then
   echo "starting worker..."
   $SPARK_HOME/sbin/start-slave.sh $ADAMPRO_MASTER
fi

if [[ $1 = "-bash" || $2 = "-bash" ]]; then
  /bin/bash
fi

while true; do sleep 60 ; done
