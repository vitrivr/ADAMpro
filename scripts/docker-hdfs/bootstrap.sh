#!/bin/bash

service ssh restart

# configuration
sed s/MASTER_HOSTNAME/$ADAMPRO_MASTER_HOSTNAME/ ${ADAMPRO_HOME}/adampro.conf.template > $ADAMPRO_HOME/adampro.conf
sed s/MASTER_HOSTNAME/$ADAMPRO_MASTER_HOSTNAME/ $HADOOP_HOME/etc/hadoop/core-site.xml.template > $HADOOP_HOME/etc/hadoop/core-site.xml
sed s/MASTER_HOSTNAME/$ADAMPRO_MASTER_HOSTNAME/ $HADOOP_HOME/etc/hadoop/yarn-site.xml.template > $HADOOP_HOME/etc/hadoop/yarn-site.xml

# hadoop
$HADOOP_HOME/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd $HADOOP_HOME/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -


if [[ $1 = "--masternode" || $2 = "--masternode" ]]; then
   echo "starting master..."

   if [ -f $HADOOP_FIRST_STARTUP ]; then
      # the first time hadoop is started, we have to format the node and set up certain directories
      # this is done in the bootstrap.sh, so that we can maintain the image same for master and worker
      echo "setting up hadoop for first time run..."
      $HADOOP_HOME/bin/hadoop namenode -format
      ${HADOOP_HOME}/sbin/start-dfs.sh
      ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /adampro/data/data
      ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /adampro/data/index
      ${HADOOP_HOME}/sbin/stop-dfs.sh

      rm $HADOOP_FIRST_STARTUP
  fi

  service dnsmasq start
  $HADOOP_PREFIX/sbin/start-yarn.sh
  $HADOOP_PREFIX/sbin/start-dfs.sh
  
  # storage engines
  if [[( -z "$ADAMPRO_START_POSTGRES" ) || ( "$ADAMPRO_START_POSTGRES" == "true")]]; then
      service postgresql stop
      su --login - postgres --command "$POSTGRES_HOME/bin/pg_ctl -w start -D $PGDATA"
  fi

  # start solr
  if [[ (-z "$ADAMPRO_START_SOLR" ) || ( "$ADAMPRO_START_SOLR" == "true")]]; then
      solr start -noprompt &
  fi

  # run ADAMpro
  $SPARK_HOME/sbin/start-master.sh
  $SPARK_HOME/bin/spark-submit --master "$ADAMPRO_MASTER" --driver-memory $ADAMPRO_DRIVER_MEMORY --executor-memory $ADAMPRO_EXECUTOR_MEMORY --conf spark.driver.port=38000 --conf spark.blockManager.port=39000 --deploy-mode client --driver-java-options "-Dlog4j.configuration=file:$ADAMPRO_HOME/log4j.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$ADAMPRO_HOME/log4j.properties" --class org.vitrivr.adampro.main.Startup $ADAMPRO_HOME/ADAMpro-assembly-0.1.0.jar &
:q:
  # start web UI
  if [[ ( -z "$ADAMPRO_START_WEBUI" ) || ( "$ADAMPRO_START_WEBUI" == "true")]]; then
      java -jar -Dlog4j.configuration=file:$ADAMPRO_HOME/log4j.properties $ADAMPRO_HOME/ADAMpro-web-assembly-0.1.0.jar &
  fi

  # start notebook
  if [[ ( -z "$ADAMPRO_START_NOTEBOOK" ) || ( "$ADAMPRO_START_NOTEBOOK" == "true")]]; then
      $SPARK_NOTEBOOK_HOME/bin/spark-notebook -Dhttp.port=10088 &
  fi
fi


if [[ $1 = "--workernode" || $2 = "--workernode" ]]; then
  echo "starting worker..."

  HN=`hostname`
  IP=`ifconfig eth0 | grep "inet addr" | awk -F: '{print $2}' | awk '{print $1}'`
  ssh $ADAMPRO_MASTER_HOSTNAME -p 2122 "grep -q -F $HN /etc/hosts || ( echo $IP $HN >> /etc/hosts && service dnsmasq restart )"

  NN=`grep $ADAMPRO_MASTER_HOSTNAME /etc/hosts | awk '{print $1}'`
  grep $ADAMPRO_MASTER_HOSTNAME /etc/resolv.conf && echo nameserver $NN > /etc/resolv.conf

  $HADOOP_PREFIX/sbin/yarn-daemons.sh start nodemanager
  $HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode

  $SPARK_HOME/sbin/start-slave.sh -m $ADAMPRO_EXECUTOR_MEMORY -p 38000 $ADAMPRO_MASTER
fi

if [[ $1 = "-d" || $2 = "-d" ]]; then
  while true; do ssh $ADAMPRO_MASTER_HOSTNAME -p 2122 cat /etc/hosts | grep -v localhost | grep -v :: | grep -v $ADAMPRO_MASTER_HOSTNAME | grep -v `hostname` | while read line ; do grep "$line" /etc/hosts > /dev/null 2>&1 || (echo "$line" >> /etc/hosts); done ; sleep 60 ; done
fi

if [[ $1 = "-bash" || $2 = "-bash" ]]; then
  /bin/bash
fi



# (graceful) shutdown
term_handler(){
   echo "*** stop ADAMpro ***"
   su --login - postgres --command "$POSTGRES_HOME/bin/pg_ctl -w stop -D $PGDATA"
   $HADOOP_PREFIX/sbin/stop-dfs.sh
   $HADOOP_PREFIX/sbin/stop-yarn.sh
   solr stop -p 8983
   exit 0;
}


# Setup signal handlers
trap 'term_handler' SIGTERM


while true
do
  tail -f /dev/null & wait ${!}
done
