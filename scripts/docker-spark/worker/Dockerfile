FROM adampar/spark-base:1.6.2-hadoop2.6

#old MAINTAINER Erika Pauwels <erika.pauwels@tenforce.com>

MAINTAINER Silvan Heller <silvan.heller@unibas.ch>

COPY worker.sh /

ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://10.34.58.136:7077"

EXPOSE 8081
RUN $HADOOP_PREFIX/bin/hdfs namenode -format

CMD ["/bin/bash", "/worker.sh"]