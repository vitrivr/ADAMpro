FROM adampar/spark-base:1.6.2-hadoop2.6

#old MAINTAINER Erika Pauwels <erika.pauwels@tenforce.com>

MAINTAINER Silvan Heller <silvan.heller@unibas.ch>

COPY worker.sh /

ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs

EXPOSE 8081
# TODO Does this work without a core-site.xml?
RUN $HADOOP_PREFIX/bin/hdfs namenode -format

CMD ["/bin/bash", "/worker.sh"]