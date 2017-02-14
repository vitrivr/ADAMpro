FROM ubuntu:16.04

# utilities
RUN apt-get -y update
RUN apt-get -y install apt-transport-https
RUN apt-get -y install curl
RUN apt-get -y install vim
RUN apt-get -y install apt-utils
RUN apt-get -y install software-properties-common

# env variables
ENV ADAM_HOME /adampro
ENV ADAM_CODE ${ADAM_HOME}/code
ENV ADAM_DATA ${ADAM_HOME}/data
ENV PGDATA ${ADAM_DATA}/data/postgres

# updating JDK to version 8
RUN add-apt-repository -y ppa:webupd8team/java && apt-get update
RUN echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections
RUN apt-get -y install oracle-java8-installer
RUN cd /usr/lib && ln -s jvm/java-8-oracle java
ENV JAVA_HOME /usr/lib/java/jre

# updating scala to 2.11.8
RUN mkdir -p /usr/lib/scala-2.11.8 && curl 'http://www.scala-lang.org/files/archive/scala-2.11.8.tgz' | tar -xz -C /usr/lib/
RUN ln -s /usr/lib/scala-2.11.8 /usr/lib/scala
ENV SCALA_HOME /usr/lib/scala
ENV PATH "$PATH:$SCALA_HOME/bin"

# sbt
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && apt-get update
RUN apt-get -y install sbt
RUN apt-get -y remove openjdk-9-*

# spark
RUN curl http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz | tar -xz -C /usr/lib/
RUN cd /usr/lib && ln -s spark-2.1.0-bin-hadoop2.7 spark
ENV SPARK_HOME /usr/lib/spark

# spark notebook
RUN curl https://s3.eu-central-1.amazonaws.com/spark-notebook/tgz/spark-notebook-0.7.0-scala-2.11.8-spark-2.1.0-hadoop-2.7.3-with-hive.tgz?max-keys=100000 | tar -xz -C /usr/lib/
RUN cd /usr/lib && ln -s spark-notebook-0.7.0-scala-2.11.8-spark-2.1.0-hadoop-2.7.3-with-hive spark-notebook
ENV SPARK_NOTEBOOK_HOME /usr/lib/spark-notebook/

# postgresql
RUN echo deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main >> /etc/apt/sources.list.d/pgdg.list && curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && apt-get update
RUN apt-get -y install postgresql-client-9.4 postgresql-9.4 postgresql-contrib-9.4 libpq-dev postgresql-server-dev-9.4
ENV POSTGRES_HOME /usr/lib/postgresql/9.4/

# solr
RUN mkdir -p /usr/lib/solr-6.1.0 && curl http://archive.apache.org/dist/lucene/solr/6.1.0/solr-6.1.0.tgz | tar -xz -C /usr/lib/
RUN apt-get -y install lsof
RUN ln -s /usr/lib/solr-6.1.0 /usr/lib/solr
ARG SOLR_HOME=/usr/lib/solr
ENV PATH "$PATH:/usr/lib/solr/bin"

# ADAMpro
RUN mkdir -p ${ADAM_HOME}
RUN chmod 777 ${ADAM_HOME}

RUN apt-get -y install git

ENV ADAMPRO_GIT "https://github.com/vitrivr/ADAMpro.git"
ENV ADAMPRO_BRANCH master
ENV ADAMPRO_MASTER local[4]

# careful: the next step is potentially cached by Docker!
RUN git clone -b $ADAMPRO_BRANCH $ADAMPRO_GIT --recursive $ADAM_CODE

# ADAMpro data
RUN mkdir -p ${ADAM_HOME}/data
COPY data/ ${ADAM_HOME}/data/
RUN if [ -d $PGDATA ]; then chown -R postgres:postgres $PGDATA && chmod -R 700 $PGDATA; else mkdir -p $PGDATA; chown -R postgres:postgres $PGDATA; su --login - postgres --command "$POSTGRES_HOME/bin/initdb -D $PGDATA; $POSTGRES_HOME/bin/pg_ctl -w start -D $PGDATA; $POSTGRES_HOME/bin/createuser -s adampro; $POSTGRES_HOME/bin/createdb adampro; $POSTGRES_HOME/bin/pg_ctl -w stop -D $PGDATA"; fi
RUN echo "host    all             all             0.0.0.0/0               trust" >> $PGDATA/pg_hba.conf && echo "listen_addresses='*'" >> $PGDATA/postgresql.conf
RUN if [ -d $ADAM_HOME/data/data/solr ]; then rm -r $SOLR_HOME/server/solr && ln -s $ADAM_HOME/data/data/solr $SOLR_HOME/server/solr; else mv $SOLR_HOME/server/solr $ADAM_HOME/data/data/ && ln -s $ADAM_HOME/data/data/solr $SOLR_HOME/server/solr; fi

RUN cd $ADAM_CODE && sbt proto && sbt assembly && sbt web/assembly
RUN cp $ADAM_CODE/conf/log4j.xml ${ADAM_HOME}/log4j.xml
RUN cp $ADAM_CODE/target/scala-2.11/ADAMpro-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-assembly-0.1.0.jar && cp $ADAM_CODE/web/target/scala-2.11/ADAMpro-web-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-web-assembly-0.1.0.jar
# COPY adampro.conf.template ${ADAM_HOME}/adampro.conf.template; this file is taken from the repository
COPY update.sh ${ADAM_HOME}/update.sh
RUN chmod 700 ${ADAM_HOME}/update.sh

RUN mkdir -p ${ADAM_HOME}/logs/

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

COPY adampro.sh ${ADAM_HOME}/adampro.sh
RUN chown root.root ${ADAM_HOME}/adampro.sh
RUN chmod 700 ${ADAM_HOME}/adampro.sh

ENTRYPOINT ["/adampro/adampro.sh"]