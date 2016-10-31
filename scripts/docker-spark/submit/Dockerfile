FROM adampar/spark-base:1.6.2-hadoop2.6

#Prev MAINTAINER Erika Pauwels <erika.pauwels@tenforce.com>
#TODO Move env-Variables to script. Having them in the dockerfile is annyoing

MAINTAINER Silvan Heller <silvan.heller@unibas.ch>

ENV SPARK_APPLICATION_JAR_LOCATION file:///target/scala-2.10/ADAMpro-assembly-0.1.0.jar
ENV SPARK_APPLICATION_MAIN_CLASS ch.unibas.dmi.dbis.adam.main.Startup
ENV SPARK_APPLICATION_ARGS ""

COPY submit.sh /

CMD ["/bin/bash", "/submit.sh"]
