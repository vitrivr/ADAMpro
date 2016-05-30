FROM sequenceiq/spark:v1.6.0onHadoop2.6.0

RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-1.6.1-bin-hadoop2.6.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-1.6.1-bin-hadoop2.6 spark
RUN $BOOTSTRAP && $HADOOP_PREFIX/bin/hadoop dfsadmin -safemode leave && $HADOOP_PREFIX/bin/hdfs dfs -put $SPARK_HOME-1.6.1-bin-hadoop2.6/lib /spark

ADD mapred-site.xml $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
ADD yarn-site.xml $HADOOP_PREFIX/etc/hadoop/yarn-site.xml

RUN curl -LO 'http://download.oracle.com/otn-pub/java/jdk/8u77-b03/jdk-8u77-linux-x64.rpm' -H 'Cookie: oraclelicense=accept-securebackup-cookie'

RUN rpm -i jdk-8u77-linux-x64.rpm
RUN rm jdk-8u77-linux-x64.rpm

ENTRYPOINT ["/etc/bootstrap.sh"]