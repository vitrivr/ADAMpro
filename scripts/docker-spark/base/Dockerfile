FROM sequenceiq/spark:v1.6.0onHadoop2.6.0

MAINTAINER Silvan Heller <silvan.heller@unibas.ch>

#Download spark with pre-compiled hadoop
RUN curl http://d3kbcqa49mib13.cloudfront.net/spark-1.6.2-bin-hadoop2.6.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-1.6.2-bin-hadoop2.6 spark

ENV HADOOP_PREFIX /usr/local/hadoop
COPY mapred-site.xml $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
COPY yarn-site.xml $HADOOP_PREFIX/etc/hadoop/yarn-site.xml
#TODO This is not optimal. Currently the idea is to distribute a core-site-template and then worker and master adjust it at docker runtime
COPY core-site-template.xml $HADOOP_PREFIX/etc/hadoop/core-site-template.xml
COPY hdfs-site.xml $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml

COPY bootstrap.sh /etc/bootstrap.sh
#Hadoop Permission modification
RUN chown root.root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ENV HADOOP_CONF_DIR $HADOOP_PREFIX/etc/hadoop

#Update to java 8
RUN curl -LO 'http://download.oracle.com/otn-pub/java/jdk/8u77-b03/jdk-8u77-linux-x64.rpm' -H 'Cookie: oraclelicense=accept-securebackup-cookie'

RUN rpm -i jdk-8u77-linux-x64.rpm
RUN rm jdk-8u77-linux-x64.rpm

#TODO Is this necessary?
ENTRYPOINT ["/etc/bootstrap.sh"]