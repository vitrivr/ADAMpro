FROM vitrivr/adampro:latest

ENV MASTER_NODE master

# ssh
RUN apt-get -y install ssh
COPY id_rsa.pub /root/.ssh/id_rsa.pub
COPY id_rsa /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa
RUN cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys

RUN sed  -i "/^[^#]*UsePAM/ s/.*/#&/"  /etc/ssh/sshd_config
RUN echo "UsePAM no" >> /etc/ssh/sshd_config
RUN echo "Port 2122" >> /etc/ssh/sshd_config
RUN echo "Port 2122" >> /etc/ssh/ssh_config
RUN echo StrictHostKeyChecking=no >> /etc/ssh/ssh_config

# hadoop
RUN curl http://d3kbcqa49mib13.cloudfront.net/hadoop-2.7.3.tar.gz | tar -xz -C /usr/lib/
RUN cd /usr/lib && ln -s hadoop-2.7.3 hadoop
RUN mkdir -p /usr/local/hadoop_work/hdfs/datanode && mkdir -p /usr/local/hadoop_work/hdfs/namenode && mkdir -p /usr/local/hadoop_work/hdfs/namesecondary

ENV HADOOP_HOME /usr/lib/hadoop
ENV LD_LIBRARY_PATH $HADOOP_HOME/lib/native/:$LD_LIBRARY_PATH
ENV PATH "$PATH:$HADOOP_HOME/bin"
ENV PATH "$PATH:$HADOOP_HOME/sbin"

ENV HADOOP_PREFIX $HADOOP_HOME
ENV HADOOP_COMMON_HOME $HADOOP_HOME
ENV HADOOP_HDFS_HOME $HADOOP_HOME
ENV HADOOP_MAPRED_HOME $HADOOP_HOME
ENV HADOOP_YARN_HOME $HADOOP_HOME
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV YARN_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV PATH $HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

COPY hadoop-core-site.xml.template $HADOOP_HOME/etc/hadoop/core-site.xml.template
COPY hadoop-hdfs-site.xml.template $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY hadoop-mapred-site.xml.template $HADOOP_HOME/etc/hadoop/mapred-site.xml
COPY hadoop-yarn-site.xml.template $HADOOP_HOME/etc/hadoop/yarn-site.xml

RUN echo export JAVA_HOME=$JAVA_HOME >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
RUN echo export HADOOP_CONF_DIR=$HADOOP_CONF_DIR >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

RUN mkdir -p /hdfs/namenode && mkdir -p /hdfs/datanode && mkdir $HADOOP_HOME/logs

RUN chmod 700 ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && chmod 700 ${HADOOP_HOME}/sbin/start-dfs.sh
RUN sed s/MASTER_NODE/localhost/ ${HADOOP_HOME}/etc/hadoop/core-site.xml.template > ${HADOOP_HOME}/etc/hadoop/core-site.xml
RUN service ssh restart && \
    ${HADOOP_HOME}/sbin/start-dfs.sh && \
    $HADOOP_HOME/bin/hadoop namenode -format && \
    ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    ${HADOOP_HOME}/sbin/start-dfs.sh && \
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /adampro/data/data && \
    ${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /adampro/data/index && \
    ${HADOOP_HOME}/sbin/stop-dfs.sh

RUN echo $MASTER_NODE >> $HADOOP_HOME/etc/hadoop/masters

# bootstrap
COPY worker.sh ${ADAM_HOME}/worker.sh
RUN chmod 700 ${ADAM_HOME}/worker.sh
