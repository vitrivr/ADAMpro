#!/bin/bash
#Reminder: In the run command, the e flags SPARK_MASTER_IP, SPARK_MASTER_PORT and HADOOP_NAMENODE

#Reference: http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html

#Set new core-site.xml
#CAREFUL: THIS WILL OVERWRITE YOUR EXISTING core-site.xml FROM WITH THE core-site-template.xml
sed  s/hadoop_master/$HADOOP_NAMENODE/ <$HADOOP_PREFIX/etc/hadoop/core-site-template.xml >$HADOOP_PREFIX/etc/hadoop/core-site.xml

bash $HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode

$HADOOP_PREFIX/bin/hdfs dfsadmin -safemode leave

bash /usr/local/spark/sbin/spark-config.sh
bash /usr/local/spark/bin/load-spark-env.sh
rm -r /spark-logs/
mkdir /spark-logs/
export SPARK_MASTER_LOG=/spark-logs/

/usr/local/spark/sbin/../bin/spark-class org.apache.spark.deploy.master.Master \
    --ip $SPARK_MASTER_IP --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG/spark-master.out
