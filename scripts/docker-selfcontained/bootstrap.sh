#!/bin/bash

# storage engines
if [[( -z "$ADAMPRO_START_POSTGRES" ) || ( "$ADAMPRO_START_POSTGRES" == "true")]]; then
    service postgresql stop
    su --login - postgres --command "$POSTGRES_HOME/bin/pg_ctl -w start -D $PGDATA"
fi

# start solr
if [[ (-z "$ADAMPRO_START_SOLR" ) || ( "$ADAMPRO_START_SOLR" == "true")]]; then
    solr start -noprompt &
fi

# start cassandra
if [[ (-z "$ADAMPRO_START_CASSANDRA" ) || ( "$ADAMPRO_START_CASSANDRA" == "true")]]; then
    /usr/local/bin/docker-entrypoint.sh &
    wait-for-it.sh
fi

# run ADAMpro
export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
$SPARK_HOME/bin/spark-submit --master "$ADAMPRO_MASTER" --driver-memory "$ADAMPRO_MEMORY" --executor-memory "$ADAMPRO_MEMORY" --deploy-mode client --driver-java-options "-Dlog4j.configuration=file:$ADAMPRO_HOME/log4j.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$ADAMPRO_HOME/log4j.properties" --conf "spark.sql.broadcastTimeout=3600" --class org.vitrivr.adampro.main.Startup $ADAMPRO_HOME/ADAMpro-assembly-0.1.0.jar &

# start web UI
if [[ ( -z "$ADAMPRO_START_WEBUI" ) || ( "$ADAMPRO_START_WEBUI" == "true")]]; then
    java -jar -Dlog4j.configuration=file:$ADAMPRO_HOME/log4j.properties $ADAMPRO_HOME/ADAMpro-web-assembly-0.1.0.jar &
fi

# start notebook
if [[ ( -z "$ADAMPRO_START_NOTEBOOK" ) || ( "$ADAMPRO_START_NOTEBOOK" == "true")]]; then
    $SPARK_NOTEBOOK_HOME/bin/spark-notebook -Dhttp.port=10088 &
fi

# startup
if [[ $1 == "-bash" ]]; then
  /bin/bash
fi

while true; do sleep 60 ; done
