#!/bin/bash

export SPARK_MASTER_IP=10.34.58.136

#Reference: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html

#HDFS Startup

bash $HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode

bash /usr/local/spark/sbin/spark-config.sh

bash /usr/local/spark/bin/load-spark-env.sh

rm -r /spark-logs/
mkdir /spark-logs/

export SPARK_MASTER_LOG=/spark-logs/

#/usr/local/hadoop/bin/hadoop fs -chmod -R 777 /

/usr/local/spark/sbin/../bin/spark-class org.apache.spark.deploy.master.Master \
    --ip $SPARK_MASTER_IP --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG/spark-master.out
