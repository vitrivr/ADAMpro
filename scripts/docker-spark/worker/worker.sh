#!/bin/bash

bash /usr/local/spark/sbin/spark-config.sh

bash /usr/local/spark/bin/load-spark-env.sh

echo "formating datanode"

$HADOOP_PREFIX/bin/hdfs datanode -format
$HADOOP_PREFIX/bin/hdfs namenode -format

echo "starting datanode"

bash $HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start datanode

$HADOOP_PREFIX/bin/hdfs dfs -put $SPARK_HOME-1.6.2-bin-hadoop2.6/lib /spark

rm -r /spark-logs/
mkdir /spark-logs/

export SPARK_WORKER_LOG=/spark-logs/

/usr/local/hadoop/bin/hadoop fs -chmod -R 777 /

/usr/local/spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG/spark-worker.out

