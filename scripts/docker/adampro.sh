#!/bin/bash

# update and build repository
$ADAM_HOME/build.sh

# configuration
sed s/HOSTNAME/$HOSTNAME/ $ADAM_HOME/code/scripts/docker/adampro.conf.template > $ADAM_HOME/adampro.conf

# storage engines
service postgresql stop
su --login - postgres --command "$POSTGRES_HOME/bin/pg_ctl -w start -D $PGDATA"

solr start -noprompt &

# run ADAMpro
export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
$SPARK_HOME/bin/spark-submit --master "local[4]" --driver-memory 2g --executor-memory 2g --deploy-mode client --driver-java-options "-Dlog4j.configuration=file:$ADAM_HOME/log4j.xml" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$ADAM_HOME/log4j.xml" --driver-java-options "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder" --conf "spark.executor.extraJavaOptions=-XX:+UnlockCommercialFeatures -XX:+FlightRecorder" --class org.vitrivr.adampro.main.Startup $ADAM_HOME/ADAMpro-assembly-0.1.0.jar &
java -jar $ADAM_HOME/ADAMpro-web-assembly-0.1.0.jar &

while true; do sleep 1000; done

