FROM adampar/spark-base:1.6.2-hadoop2.6

# based on the big-data-europe master container
# Old Version MAINTAINER Erika Pauwels <erika.pauwels@tenforce.com>

MAINTAINER Silvan Heller <silvan.heller@unibas.ch>

COPY master.sh /

ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

# Maybe re-add in the future with yarn RUN $BOOTSTRAP

EXPOSE 8080 7077 6066

# TODO Does this work without a core-site.xml?

RUN $HADOOP_PREFIX/bin/hdfs namenode -format

CMD ["/bin/bash", "/master.sh"]