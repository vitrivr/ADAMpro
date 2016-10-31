#!/bin/bash
#Reminder: In the run command, the e flags HADOOP_NAMENODE and SPARK_MASTER should be provided

#Set new core-site.xml
#CAREFUL: THIS WILL OVERWRITE YOUR EXISTING core-site.xml FROM WITH THE core-site-template.xml
sed s/hadoop_master/$HADOOP_NAMENODE/ <$HADOOP_PREFIX/etc/hadoop/core-site-template.xml >$HADOOP_PREFIX/etc/hadoop/core-site.xml

bash $HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start datanode

bash /usr/local/spark/sbin/spark-config.sh
bash /usr/local/spark/bin/load-spark-env.sh

$HADOOP_PREFIX/bin/hdfs dfs -put $SPARK_HOME/lib /spark

rm -r /spark-logs/
mkdir /spark-logs/
export SPARK_WORKER_LOG=/spark-logs/

/usr/local/spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG/spark-worker.out

