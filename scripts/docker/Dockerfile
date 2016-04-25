FROM sequenceiq/spark:v1.6.0onHadoop2.6.0

ADD mapred-site.xml $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
ADD yarn-site.xml $HADOOP_PREFIX/etc/hadoop/yarn-site.xml

RUN curl -LO 'http://download.oracle.com/otn-pub/java/jdk/8u77-b03/jdk-8u77-linux-x64.rpm' -H 'Cookie: oraclelicense=accept-securebackup-cookie'

RUN rpm -i jdk-8u77-linux-x64.rpm
RUN rm jdk-8u77-linux-x64.rpm