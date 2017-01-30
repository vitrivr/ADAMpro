#!/bin/bash

# configuration
sed s/HOSTNAME/$HOSTNAME/ $ADAM_HOME/adampro.conf.template > $ADAM_HOME/adampro.conf

# storage engines
service postgresql stop
su --login - postgres --command "$POSTGRES_HOME/bin/pg_ctl -w start -D $PGDATA"

solr start -noprompt &

# run ADAMpro
$SPARK_HOME/bin/spark-submit --master "local[4]" --driver-memory 5g --executor-memory 5g --deploy-mode client --class org.vitrivr.adampro.main.Startup $ADAM_HOME/ADAMpro-assembly-0.1.0.jar &
java -jar $ADAM_HOME/ADAMpro-web-assembly-0.1.0.jar &

while true; do sleep 1000; done

