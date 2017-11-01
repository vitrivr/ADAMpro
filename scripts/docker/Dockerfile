FROM ubuntu:16.04

# utilities
RUN apt-get -y update; apt-get -y install apt-transport-https curl vim apt-utils software-properties-common git

# env variables
ENV ADAMPRO_HOME /adampro
ENV ADAMPRO_CODE ${ADAMPRO_HOME}/code
ENV ADAMPRO_DATA ${ADAMPRO_HOME}/data
ENV PGDATA ${ADAMPRO_DATA}/data/postgres

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

# spark notebook
RUN curl https://s3.eu-central-1.amazonaws.com/spark-notebook/tgz/spark-notebook-0.7.0-scala-2.11.8-spark-2.1.0-hadoop-2.7.3-with-hive.tgz?max-keys=100000 | tar -xz -C /usr/lib/; \
    cd /usr/lib && ln -s spark-notebook-0.7.0-scala-2.11.8-spark-2.1.0-hadoop-2.7.3-with-hive spark-notebook
ENV SPARK_NOTEBOOK_HOME /usr/lib/spark-notebook/

# postgresql
RUN echo deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main >> /etc/apt/sources.list.d/pgdg.list && curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && apt-get update; \
    apt-get -y install postgresql-client-9.4 postgresql-9.4 postgresql-contrib-9.4 libpq-dev postgresql-server-dev-9.4
ENV POSTGRES_HOME /usr/lib/postgresql/9.4/

# solr
RUN mkdir -p /usr/lib/solr-6.1.0 && curl http://archive.apache.org/dist/lucene/solr/6.1.0/solr-6.1.0.tgz | tar -xz -C /usr/lib/; \
    apt-get -y install lsof; \
    ln -s /usr/lib/solr-6.1.0 /usr/lib/solr
ARG SOLR_HOME=/usr/lib/solr
ENV PATH "$PATH:/usr/lib/solr/bin"

# ADAMpro
RUN mkdir -p ${ADAMPRO_HOME}; chmod 777 ${ADAMPRO_HOME}

ENV ADAMPRO_GIT "https://github.com/vitrivr/ADAMpro.git"
ENV ADAMPRO_BRANCH master
ENV ADAMPRO_MASTER local[4]

# careful: the next step is potentially cached by Docker!
RUN git clone -b $ADAMPRO_BRANCH $ADAMPRO_GIT --recursive $ADAMPRO_CODE

# ADAMpro data
RUN mkdir -p ${ADAMPRO_HOME}/data; mkdir -p ${ADAMPRO_HOME}/logs/
COPY data/ ${ADAMPRO_HOME}/data/
RUN if [ -d $PGDATA ]; then chown -R postgres:postgres $PGDATA && chmod -R 700 $PGDATA; else mkdir -p $PGDATA; chown -R postgres:postgres $PGDATA; su --login - postgres --command "$POSTGRES_HOME/bin/initdb -D $PGDATA; $POSTGRES_HOME/bin/pg_ctl -w start -D $PGDATA; $POSTGRES_HOME/bin/createuser -s adampro; $POSTGRES_HOME/bin/createdb adampro; $POSTGRES_HOME/bin/pg_ctl -w stop -D $PGDATA"; fi; \
    echo "host    all             all             0.0.0.0/0               trust" >> $PGDATA/pg_hba.conf && echo "listen_addresses='*'" >> $PGDATA/postgresql.conf; \
    if [ -d $ADAMPRO_HOME/data/data/solr ]; then rm -r $SOLR_HOME/server/solr && ln -s $ADAMPRO_HOME/data/data/solr $SOLR_HOME/server/solr; else mv $SOLR_HOME/server/solr $ADAMPRO_HOME/data/data/ && ln -s $ADAMPRO_HOME/data/data/solr $SOLR_HOME/server/solr; fi

RUN cd $ADAMPRO_CODE && sbt proto && sbt assembly && sbt web/assembly; \
    cp $ADAMPRO_CODE/conf/log4j.xml ${ADAMPRO_HOME}/log4j.xml; \
    cp $ADAMPRO_CODE/target/scala-2.11/ADAMpro-assembly-0.1.0.jar ${ADAMPRO_HOME}/ADAMpro-assembly-0.1.0.jar && cp $ADAMPRO_CODE/web/target/scala-2.11/ADAMpro-web-assembly-0.1.0.jar ${ADAMPRO_HOME}/ADAMpro-web-assembly-0.1.0.jar
# COPY adampro.conf.template ${ADAMPRO_HOME}/adampro.conf.template; this file is taken from the repository
COPY update.sh ${ADAMPRO_HOME}/update.sh
RUN chmod 700 ${ADAMPRO_HOME}/update.sh

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
# port for spark notebook
EXPOSE 10088

# bootstrap
ENV ADAMPRO_START_POSTGRES true
ENV ADAMPRO_START_SOLR true
ENV ADAMPRO_START_WEBUI true
ENV ADAMPRO_START_NOTEBOOK true
ENV ADAMPRO_MEMORY 2g

COPY bootstrap.sh ${ADAMPRO_HOME}/bootstrap.sh
RUN chown root.root ${ADAMPRO_HOME}/bootstrap.sh; chmod 700 ${ADAMPRO_HOME}/bootstrap.sh

ENTRYPOINT ["/adampro/bootstrap.sh"]