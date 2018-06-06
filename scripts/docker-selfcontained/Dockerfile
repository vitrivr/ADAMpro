FROM cassandra:3.11

# utilities
RUN apt-get -y update; apt-get -y install apt-transport-https bc curl vim apt-utils software-properties-common git

# env variables
ENV ADAMPRO_HOME /adampro
ENV ADAMPRO_CODE ${ADAMPRO_HOME}/code
ENV ADAMPRO_DATA ${ADAMPRO_HOME}/data

RUN  mkdir -p /usr/lib/java && curl -v -j -k -L -H "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie"   http://download.oracle.com/otn-pub/java/jdk/10.0.1+10/fb4372174a714e6b8c52526dc134031e/jdk-10.0.1_linux-x64_bin.tar.gz | tar -xz -C /usr/lib/java --strip-components=1; \
    ln -s /usr/lib/java /usr/lib/jvm/jre 

RUN update-alternatives --install "/usr/bin/java" "java" "/usr/lib/java/bin/java" 1; \
    update-alternatives --install "/usr/bin/javac" "javac" "/usr/lib/java/bin/javac" 1; \
    update-alternatives --install "/usr/bin/javaws" "javaws" "/usr/lib/java/bin/javaws" 1; \
    chmod a+x /usr/bin/java; \
    chmod a+x /usr/bin/javac; \
    chmod a+x /usr/bin/javaws; \
    chown -R root:root /usr/lib/java

RUN update-alternatives --set java /usr/lib/java/bin/java; \
    update-alternatives --set javac /usr/lib/java/bin/javac; \
    update-alternatives --set javaws /usr/lib/java/bin/javaws

# updating scala to 2.11
RUN mkdir -p /usr/lib/scala-2.11.8 && curl 'http://www.scala-lang.org/files/archive/scala-2.11.8.tgz' | tar -xz -C /usr/lib/; \
    ln -s /usr/lib/scala-2.11.8 /usr/lib/scala
ENV SCALA_HOME /usr/lib/scala
ENV PATH "$PATH:$SCALA_HOME/bin"

# sbt
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list; \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 && apt-get update; \
    apt-get -y install sbt;

# spark
RUN curl http://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz | tar -xz -C /usr/lib/; \
    cd /usr/lib && ln -s spark-2.2.0-bin-hadoop2.7 spark
ENV SPARK_HOME /usr/lib/spark

# spark notebook
RUN curl https://s3.eu-central-1.amazonaws.com/spark-notebook/tgz/spark-notebook-0.7.0-scala-2.11.8-spark-2.1.0-hadoop-2.7.3-with-hive.tgz?max-keys=100000 | tar -xz -C /usr/lib/; \
    cd /usr/lib && ln -s spark-notebook-0.7.0-scala-2.11.8-spark-2.1.0-hadoop-2.7.3-with-hive spark-notebook
ENV SPARK_NOTEBOOK_HOME /usr/lib/spark-notebook/

# postgresql
ENV PGDATA ${ADAMPRO_DATA}/data/postgres
RUN echo deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main >> /etc/apt/sources.list.d/pgdg.list && curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && apt-get update; \
    apt-get -y install postgresql-client-9.4 postgresql-9.4 postgresql-contrib-9.4 libpq-dev postgresql-server-dev-9.4
ENV POSTGRES_HOME /usr/lib/postgresql/9.4/

# solr
RUN mkdir -p /usr/lib/solr-6.1.0 && curl http://archive.apache.org/dist/lucene/solr/6.1.0/solr-6.1.0.tgz | tar -xz -C /usr/lib/; \
    apt-get -y install lsof; \
    ln -s /usr/lib/solr-6.1.0 /usr/lib/solr
ARG SOLR_HOME=/usr/lib/solr
ENV PATH "$PATH:/usr/lib/solr/bin"

# cassandra
ENV CASSANDRADATA ${ADAMPRO_DATA}/data/cassandra
RUN sed -i.bak 's/var\/lib/usr\/lib/' /etc/cassandra/cassandra.yaml && chmod 700 /etc/cassandra/cassandra.yaml

# ADAMpro
RUN mkdir -p ${ADAMPRO_HOME}; chmod 777 ${ADAMPRO_HOME}

ENV ADAMPRO_GIT "https://github.com/vitrivr/ADAMpro.git"
ENV ADAMPRO_BRANCH "master"

# careful: the next step is potentially cached by Docker!
RUN git clone -b $ADAMPRO_BRANCH $ADAMPRO_GIT --recursive $ADAMPRO_CODE

RUN cd $ADAMPRO_CODE && sbt proto && sbt assembly && sbt web/assembly; \
    cp $ADAMPRO_CODE/conf/log4j.xml ${ADAMPRO_HOME}/log4j.xml; \
    cp $ADAMPRO_CODE/conf/log4j.properties ${ADAMPRO_HOME}/log4j.properties; \
    cp $ADAMPRO_CODE/target/scala-2.11/ADAMpro-assembly-0.1.0.jar ${ADAMPRO_HOME}/ADAMpro-assembly-0.1.0.jar && cp $ADAMPRO_CODE/web/target/scala-2.11/ADAMpro-web-assembly-0.1.0.jar ${ADAMPRO_HOME}/ADAMpro-web-assembly-0.1.0.jar
COPY update.sh ${ADAMPRO_HOME}/update.sh
RUN chmod 700 ${ADAMPRO_HOME}/update.sh

# ADAMpro data
RUN mkdir -p ${ADAMPRO_HOME}/data; mkdir -p ${ADAMPRO_HOME}/logs/
COPY data/ ${ADAMPRO_HOME}/data/
RUN if [ -d $PGDATA ]; then chown -R postgres:postgres $PGDATA && chmod -R 700 $PGDATA; else mkdir -p $PGDATA; chown -R postgres:postgres $PGDATA; su --login - postgres --command "$POSTGRES_HOME/bin/initdb -D $PGDATA; $POSTGRES_HOME/bin/pg_ctl -w start -D $PGDATA; $POSTGRES_HOME/bin/createuser -s adampro; $POSTGRES_HOME/bin/createdb adampro; $POSTGRES_HOME/bin/pg_ctl -w stop -D $PGDATA"; fi; \
    echo "host    all             all             0.0.0.0/0               trust" >> $PGDATA/pg_hba.conf && echo "listen_addresses='*'" >> $PGDATA/postgresql.conf; \
    if [ -d $ADAMPRO_HOME/data/data/solr ]; then rm -r $SOLR_HOME/server/solr && ln -s $ADAMPRO_HOME/data/data/solr $SOLR_HOME/server/solr; else mv $SOLR_HOME/server/solr $ADAMPRO_HOME/data/data/ && ln -s $ADAMPRO_HOME/data/data/solr $SOLR_HOME/server/solr; fi
RUN mkdir -p $CASSANDRADATA && chown -R cassandra:cassandra $CASSANDRADATA && chmod 777 $CASSANDRADATA && ln -s $CASSANDRADATA /usr/lib/cassandra

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

COPY adampro.conf ${ADAMPRO_HOME}/adampro.conf
RUN chown root.root ${ADAMPRO_HOME}/adampro.conf; chmod 700 ${ADAMPRO_HOME}/adampro.conf
COPY bootstrap.sh ${ADAMPRO_HOME}/bootstrap.sh
RUN chown root.root ${ADAMPRO_HOME}/bootstrap.sh; chmod 700 ${ADAMPRO_HOME}/bootstrap.sh

ENTRYPOINT ["/adampro/bootstrap.sh"]