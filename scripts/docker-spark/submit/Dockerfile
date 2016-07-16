FROM adampar/spark-base:1.6.2-hadoop2.6

#Prev MAINTAINER Erika Pauwels <erika.pauwels@tenforce.com>
#TODO Move env-Variables to script. Having them in the dockerfile is annyoing

MAINTAINER Silvan Heller <silvan.heller@unibas.ch>

ENV SPARK_MASTER_NAME 10.34.58.136
ENV SPARK_MASTER_PORT 7077

ENV SPARK_APPLICATION_JAR_LOCATION file:///target/scala-2.10/ADAMpro-assembly-0.1.0.jar
ENV SPARK_APPLICATION_MAIN_CLASS ch.unibas.dmi.dbis.adam.main.Startup
ENV SPARK_APPLICATION_ARGS ""

RUN cp /usr/local/hadoop/etc/hadoop/core-site.xml /usr/local/spark/conf/core-site.xml

COPY submit.sh /

CMD ["/bin/bash", "/submit.sh"]
