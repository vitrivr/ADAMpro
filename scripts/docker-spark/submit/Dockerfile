FROM adampar/spark-base:1.6.2-hadoop2.6

#Prev MAINTAINER Erika Pauwels <erika.pauwels@tenforce.com>

MAINTAINER Silvan Heller <silvan.heller@unibas.ch>

ENV SPARK_MASTER_NAME 10.34.58.136
ENV SPARK_MASTER_PORT 7077
#TODO
ENV SPARK_APPLICATION_JAR_LOCATION /app/application.jar
ENV SPARK_APPLICATION_MAIN_CLASS ch.unibas.dmi.dbis.adam.main.Startup
ENV SPARK_APPLICATION_ARGS ""

COPY submit.sh /

CMD ["/bin/bash", "/submit.sh"]
