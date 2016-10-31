#!/bin/bash

echo "Submit application ${SPARK_APPLICATION_JAR_LOCATION} with main class ${SPARK_APPLICATION_MAIN_CLASS} to Spark master ${SPARK_MASTER_URL}"
echo "Passing arguments ${SPARK_APPLICATION_ARGS}"

sed  s/hadoop_master/$HADOOP_NAMENODE/ <$HADOOP_PREFIX/etc/hadoop/core-site-template.xml >$HADOOP_PREFIX/etc/hadoop/core-site.xml

cat $HADOOP_PREFIX/etc/hadoop/core-site.xml

#TODO this can be spark_home, right?
cp $HADOOP_PREFIX/etc/hadoop/core-site.xml /usr/local/spark/conf/core-site.xml
cp /target/conf/log4j.properties /usr/local/spark/conf/log4j.properties

/usr/local/spark/bin/spark-submit \
    --class ${SPARK_APPLICATION_MAIN_CLASS} \
    --master ${SPARK_MASTER_URL} \
    --conf spark.driver.port=8087 \
    --conf spark.blockManager.port=50543 \
    --conf spark.fileserver.port=47957\
    ${SPARK_APPLICATION_JAR_LOCATION} ${SPARK_APPLICATION_ARGS}


##TODO We want the entry-point to be the command-line so we can spark-submit
CMD=${1:-"exit 0"}
if [[ "$CMD" == "-d" ]];
then
	service sshd stop
	/usr/sbin/sshd -D -d
else
	/bin/bash -c "$*"
fi
