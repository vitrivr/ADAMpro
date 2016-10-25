FROM sequenceiq/spark:v1.6.0onHadoop2.6.0

# spark
ENV SPARK_HOME /usr/local/spark
RUN curl http://d3kbcqa49mib13.cloudfront.net/spark-1.6.2-bin-hadoop2.6.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-1.6.2-bin-hadoop2.6 spark
RUN $BOOTSTRAP && $HADOOP_PREFIX/bin/hadoop dfsadmin -safemode leave && $HADOOP_PREFIX/bin/hdfs dfs -put $SPARK_HOME/lib /spark

ADD mapred-site.xml $HADOOP_PREFIX/etc/hadoop/mapred-site.xml
ADD yarn-site.xml $HADOOP_PREFIX/etc/hadoop/yarn-site.xml

# updating JDK to version 8
RUN curl -LO 'http://download.oracle.com/otn-pub/java/jdk/8u77-b03/jdk-8u77-linux-x64.rpm' -H 'Cookie: oraclelicense=accept-securebackup-cookie'
RUN rpm -i jdk-8u77-linux-x64.rpm
RUN rm jdk-8u77-linux-x64.rpm

# postgresql
ENV PGDATA /var/lib/pgsql/data
RUN rpm -Uvh http://yum.postgresql.org/9.4/redhat/rhel-6-x86_64/pgdg-redhat94-9.4-1.noarch.rpm
RUN yum -y update postgresql94-server postgresql94
RUN yum -y install postgresql94-server postgresql94
RUN service postgresql-9.4 stop

USER postgres
RUN /usr/pgsql-9.4/bin/initdb --encoding=UTF8
RUN echo "host    all             all             0.0.0.0/0               trust" >> $PGDATA/pg_hba.conf
RUN echo "listen_addresses='*'" >> $PGDATA/postgresql.conf
## create adampro postgresql database
RUN /usr/pgsql-9.4/bin/pg_ctl start &&\sleep 5 &&\/usr/pgsql-9.4/bin/psql --command "CREATE USER adampro WITH SUPERUSER PASSWORD 'adampro';" &&\/usr/pgsql-9.4/bin/createdb -O adampro adampro

USER root

# solr
RUN curl  http://archive.apache.org/dist/lucene/solr/6.1.0/solr-6.1.0.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s solr-6.1.0 solr
RUN yum -y install lsof
COPY solr /etc/init.d/solr
RUN chmod 777 /etc/init.d/solr
RUN chkconfig --add solr

# adampro
ENV ADAM_HOME /usr/local/adam

# adding jar files
RUN mkdir -p $ADAM_HOME
RUN chmod 777 $ADAM_HOME
COPY ADAMpro-assembly-0.1.0.jar $ADAM_HOME/ADAMpro-assembly-0.1.0.jar
COPY ADAMpro-web-assembly-0.1.0.jar $ADAM_HOME/ADAMpro-web-assembly-0.1.0.jar
COPY adampro.conf.template $ADAM_HOME/adampro.conf.template

# adding data
# RUN mkdir -p /adampro
# COPY adampro.tar.gz /adampro/adampro.tar.gz
# RUN tar -xvf /adampro/adampro.tar.gz -C /adampro
# RUN $BOOTSTRAP && $HADOOP_PREFIX/bin/hadoop dfsadmin -safemode leave && $HADOOP_PREFIX/bin/hdfs dfs -mkdir /adampro && $HADOOP_PREFIX/bin/hdfs dfs -copyFromLocal /adampro/data /adampro && $HADOOP_PREFIX/bin/hdfs dfs -copyFromLocal /adampro/index /adampro
# RUN rm -r $PGDATA && cp -R /adampro/postgres $PGDATA && chown -R postgres:postgres $PGDATA/.. && chmod -R 700 $PGDATA

# adjust logging
RUN sed s/INFO/WARN/ $SPARK_HOME/conf/log4j.properties.template > $SPARK_HOME/conf/log4j.properties
RUN echo 'log4j.logger.ch=DEBUG' >> $SPARK_HOME/conf/log4j.properties

#port for postgresql
EXPOSE 5432
# port for spark UI
EXPOSE 4040
# port for solr
EXPOSE 8983
# port for grpc
EXPOSE 5890 
# port for ADAMpro web UI
EXPOSE 9099

# bootstrap
COPY adampro.sh /etc/adampro.sh
RUN chown root.root /etc/adampro.sh
RUN chmod 700 /etc/adampro.sh
ENTRYPOINT ["/etc/adampro.sh"]