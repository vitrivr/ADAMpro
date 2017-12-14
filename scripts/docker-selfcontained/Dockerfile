FROM vitrivr/adampro:latest

# env variables
ENV ADAMPRO_HOME /adampro
ENV ADAMPRO_CODE ${ADAMPRO_HOME}/code
ENV ADAMPRO_DATA ${ADAMPRO_HOME}/data
ENV PGDATA ${ADAMPRO_DATA}/data/postgres

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

# ADAMpro data
RUN mkdir -p ${ADAMPRO_HOME}/data; mkdir -p ${ADAMPRO_HOME}/logs/
COPY data/ ${ADAMPRO_HOME}/data/
RUN if [ -d $PGDATA ]; then chown -R postgres:postgres $PGDATA && chmod -R 700 $PGDATA; else mkdir -p $PGDATA; chown -R postgres:postgres $PGDATA; su --login - postgres --command "$POSTGRES_HOME/bin/initdb -D $PGDATA; $POSTGRES_HOME/bin/pg_ctl -w start -D $PGDATA; $POSTGRES_HOME/bin/createuser -s adampro; $POSTGRES_HOME/bin/createdb adampro; $POSTGRES_HOME/bin/pg_ctl -w stop -D $PGDATA"; fi; \
    echo "host    all             all             0.0.0.0/0               trust" >> $PGDATA/pg_hba.conf && echo "listen_addresses='*'" >> $PGDATA/postgresql.conf; \
    if [ -d $ADAMPRO_HOME/data/data/solr ]; then rm -r $SOLR_HOME/server/solr && ln -s $ADAMPRO_HOME/data/data/solr $SOLR_HOME/server/solr; else mv $SOLR_HOME/server/solr $ADAMPRO_HOME/data/data/ && ln -s $ADAMPRO_HOME/data/data/solr $SOLR_HOME/server/solr; fi

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