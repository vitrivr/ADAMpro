FROM adampar/spark-base:1.6.2-hadoop2.6

# Old Version MAINTAINER Erika Pauwels <erika.pauwels@tenforce.com>

MAINTAINER Silvan Heller <silvan.heller@unibas.ch>

COPY master.sh /

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/master.sh"]