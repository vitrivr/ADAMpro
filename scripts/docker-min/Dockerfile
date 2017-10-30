FROM ubuntu:16.04

# utilities
RUN apt-get -y update; apt-get -y install apt-transport-https bc curl vim apt-utils software-properties-common git

# env variables
ENV ADAM_HOME /adampro
ENV ADAM_CODE ${ADAM_HOME}/code
ENV ADAM_DATA ${ADAM_HOME}/data

# updating JDK to version 8
RUN add-apt-repository -y ppa:webupd8team/java && apt-get update; \
    echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections; \
    apt-get -y install oracle-java8-installer; \
    cd /usr/lib && ln -s jvm/java-8-oracle java
ENV JAVA_HOME /usr/lib/java/jre

# updating scala to 2.11
RUN mkdir -p /usr/lib/scala-2.11.8 && curl 'http://www.scala-lang.org/files/archive/scala-2.11.8.tgz' | tar -xz -C /usr/lib/; \
    ln -s /usr/lib/scala-2.11.8 /usr/lib/scala
ENV SCALA_HOME /usr/lib/scala
ENV PATH "$PATH:$SCALA_HOME/bin"

# sbt
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list; \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && apt-get update; \
    apt-get -y install sbt; \
    apt-get -y remove openjdk-9-*

# spark
RUN curl http://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz | tar -xz -C /usr/lib/; \
    cd /usr/lib && ln -s spark-2.2.0-bin-hadoop2.7 spark
ENV SPARK_HOME /usr/lib/spark

# ADAMpro
RUN mkdir -p ${ADAM_HOME}; chmod 777 ${ADAM_HOME}

ENV ADAMPRO_GIT "https://github.com/vitrivr/ADAMpro.git"
ENV ADAMPRO_BRANCH "dev_v.2.1"

# careful: the next step is potentially cached by Docker!
RUN git clone -b $ADAMPRO_BRANCH $ADAMPRO_GIT --recursive $ADAM_CODE

RUN cd $ADAM_CODE && sbt proto && sbt assembly && sbt web/assembly; \
    cp $ADAM_CODE/conf/log4j.xml ${ADAM_HOME}/log4j.xml; \
    cp $ADAM_CODE/target/scala-2.11/ADAMpro-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-assembly-0.1.0.jar && cp $ADAM_CODE/web/target/scala-2.11/ADAMpro-web-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-web-assembly-0.1.0.jar
COPY update.sh ${ADAM_HOME}/update.sh
RUN chmod 700 ${ADAM_HOME}/update.sh

# port for spark UI
EXPOSE 4040
# port for grpc
EXPOSE 5890
# port for ADAMpro web UI
EXPOSE 9099

# bootstrap
ARG MASTER_NODE
ENV MASTER_HOSTNAME $MASTER_NODE
ENV ADAMPRO_MASTER spark://$MASTER_HOSTNAME:7077

ENV ADAMPRO_START_WEBUI true
ENV ADAMPRO_MEMORY 2g

COPY wait-for-it.sh ${ADAM_HOME}/wait-for-it.sh
COPY bootstrap.sh ${ADAM_HOME}/bootstrap.sh
RUN chown root.root ${ADAM_HOME}/wait-for-it.sh; chmod 700 ${ADAM_HOME}/wait-for-it.sh; chown root.root ${ADAM_HOME}/bootstrap.sh; chmod 700 ${ADAM_HOME}/bootstrap.sh

ENTRYPOINT ["/adampro/bootstrap.sh"]