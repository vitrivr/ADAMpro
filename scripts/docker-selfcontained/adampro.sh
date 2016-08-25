#!/bin/bash

service postgresql-9.4 stop

: ${HADOOP_PREFIX:=/usr/local/hadoop}

$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd $HADOOP_PREFIX/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -

# altering configurations
sed s/HOSTNAME/$HOSTNAME/ /usr/local/hadoop/etc/hadoop/core-site.xml.template > /usr/local/hadoop/etc/hadoop/core-site.xml
sed s/HOSTNAME/$HOSTNAME/ $ADAM_HOME/adampro.conf.template > $ADAM_HOME/adampro.conf

# setting spark defaults
echo spark.yarn.jar hdfs:///localhost/spark-assembly-1.6.0-hadoop2.6.0.jar > $SPARK_HOME/conf/spark-defaults.conf
cp $SPARK_HOME/conf/metrics.properties.template $SPARK_HOME/conf/metrics.properties

service sshd start
$HADOOP_PREFIX/sbin/start-dfs.sh
$HADOOP_PREFIX/sbin/start-yarn.sh

# postgresql
su --login - postgres --command "/usr/pgsql-9.4/bin/pg_ctl -w start -D $PGDATA"

# run ADAMpro
sleep 10 #sleep until HDFS is started

hdfs dfsadmin -safemode leave
$SPARK_HOME/bin/spark-submit --master "local[4]" --deploy-mode client --class ch.unibas.dmi.dbis.adam.main.Startup $ADAM_HOME/ADAMpro-assembly-0.1.0.jar &

sleep 10 #sleep until ADAMpro is started

# solr
service solr start

java -jar $ADAM_HOME/ADAMpro-web-assembly-0.1.0.jar

# start container always with option -d
service sshd stop
/usr/sbin/sshd -D -d