FROM ubuntu:16.04

# utilities
RUN apt-get -y update
RUN apt-get -y apt-transport-https
RUN apt-get -y install curl
RUN apt-get -y install vim

# env variables
ENV ADAM_HOME /adampro
ENV ADAM_CODE ${ADAM_HOME}/code
ENV ADAM_DATA ${ADAM_HOME}/data
ENV PGDATA ${ADAM_DATA}/data/postgres

# spark
RUN curl http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.6.tgz | tar -xz -C /usr/lib/
RUN cd /usr/lib && ln -s spark-2.1.0-bin-hadoop2.6 spark
ENV SPARK_HOME /usr/lib/spark

# updating JDK to version 8
RUN mkdir -p /usr/lib/jdk1.8.0_77 && curl -LO 'http://download.oracle.com/otn-pub/java/jdk/8u77-b03/jdk-8u77-linux-x64.tar.gz' -H 'Cookie: oraclelicense=accept-securebackup-cookie'
RUN tar xvfz jdk-8u77-linux-x64.tar.gz -C /usr/lib/
RUN ln -s /usr/lib/jdk1.8.0_77 /usr/lib/java
ENV JAVA_HOME /usr/lib/java
ENV PATH "$PATH:/usr/lib/java/bin"

# updating scala to 2.11.8
RUN mkdir -p /usr/lib/scala-2.11.8 && curl 'http://www.scala-lang.org/files/archive/scala-2.11.8.tgz' | tar -xz -C /usr/lib/
RUN ln -s /usr/lib/scala-2.11.8 /usr/lib/scala
ENV SCALA_HOME /usr/lib/scala
ENV PATH "$PATH:$SCALA_HOME/bin"

# sbt
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && apt-get update
RUN apt-get -y install sbt

# postgresql
RUN echo deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main >> /etc/apt/sources.list.d/pgdg.list && curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && apt-get update
RUN apt-get -y install postgresql-client-9.4 postgresql-9.4 postgresql-contrib-9.4 libpq-dev postgresql-server-dev-9.4
ENV POSTGRES_HOME /usr/lib/postgresql/9.4/

# solr
RUN mkdir -p /usr/lib/solr-6.1.0 && curl http://archive.apache.org/dist/lucene/solr/6.1.0/solr-6.1.0.tgz | tar -xz -C /usr/lib/
RUN apt-get -y install lsof
RUN ln -s /usr/lib/solr-6.1.0 /usr/lib/solr
ENV PATH "$PATH:/usr/lib/solr/bin"

# adampro
# adding jar files
RUN mkdir -p ${ADAM_HOME}
RUN chmod 777 ${ADAM_HOME}
RUN apt-get -y install git

# TODO: build jars from github repository
COPY ADAMpro-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-assembly-0.1.0.jar
COPY ADAMpro-web-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-web-assembly-0.1.0.jar
COPY adampro.conf.template ${ADAM_HOME}/adampro.conf.template
COPY log4j2.xml ${ADAM_HOME}/log4j2.xml

RUN mkdir -p ${ADAM_HOME}/logs/

# adding data
RUN mkdir -p ${ADAM_HOME}/data
COPY adampro.tar.gz /adampro/data.tar.gz
RUN tar -xvf $ADAM_HOME/data.tar.gz -C $ADAM_HOME/data
RUN chown -R postgres:postgres $ADAM_HOME/data/data/postgres && chmod -R 700 $PGDATA

# port for postgresql
EXPOSE 5432
# port for solr
EXPOSE 8983
# port for spark UI
EXPOSE 4040
# port for grpc
EXPOSE 5890
# port for ADAMpro web UI
EXPOSE 9099

# bootstrap
COPY adampro.sh ${ADAM_HOME}/adampro.sh
RUN chown root.root ${ADAM_HOME}/adampro.sh
RUN chmod 700 ${ADAM_HOME}/adampro.sh

ENTRYPOINT ["/adampro/adampro.sh"]