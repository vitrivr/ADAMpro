#!/bin/bash

service ssh restart

# configuration
sed s/HOSTNAME/$HOSTNAME/ ${ADAM_HOME}/adampro.conf.template > $ADAM_HOME/adampro.conf
sed s/MASTER_NODE/$HOSTNAME/ $HADOOP_HOME/etc/hadoop/core-site.xml.template > $HADOOP_HOME/etc/hadoop/core-site.xml

# hadoop
$HADOOP_HOME/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd $HADOOP_HOME/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -

$HADOOP_HOME/sbin/start-all.sh

# storage engines
service postgresql stop
su --login - postgres --command "$POSTGRES_HOME/bin/pg_ctl -w start -D $PGDATA"

# run ADAMpro
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/bin/spark-submit --master "$ADAMPRO_MASTER" --driver-memory 4g --deploy-mode client --driver-java-options "-Dlog4j.configuration=file:$ADAM_HOME/log4j.xml" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$ADAM_HOME/log4j.xml" --driver-java-options "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder" --conf "spark.executor.extraJavaOptions=-XX:+UnlockCommercialFeatures -XX:+FlightRecorder" --class org.vitrivr.adampro.main.Startup $ADAM_HOME/ADAMpro-assembly-0.1.0.jar &

# start web UI
java -jar $ADAM_HOME/ADAMpro-web-assembly-0.1.0.jar &

while true; do sleep 1000; done

