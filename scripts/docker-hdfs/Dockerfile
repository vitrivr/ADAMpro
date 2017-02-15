# based on https://github.com/sfedyakov/hadoop-271-cluster

FROM vitrivr/adampro:latest

ARG MASTER_NODE
ENV MASTER_HOSTNAME $MASTER_NODE
ENV ADAMPRO_MASTER spark://$MASTER_HOSTNAME:7077

# ssh, dnsmasq
RUN apt-get -y install ssh
COPY id_rsa.pub /root/.ssh/id_rsa.pub
COPY id_rsa /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa
RUN cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys

RUN sed  -i "/^[^#]*UsePAM/ s/.*/#&/"  /etc/ssh/sshd_config && \
    echo "UsePAM no" >> /etc/ssh/sshd_config && \
    echo "Port 2122" >> /etc/ssh/sshd_config
ADD ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/config && \
    chown root:root /root/.ssh/config

RUN apt-get -y install dnsmasq snappy openssl net-tools && \
    echo user=root >> /etc/dnsmasq.conf && \
    echo bogus-priv >> /etc/dnsmasq.conf && \
    echo interface=eth0 >> /etc/dnsmasq.conf && \
    echo no-dhcp-interface=eth0 >> /etc/dnsmasq.conf

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
COPY hadoop-yarn-site.xml.template $HADOOP_HOME/etc/hadoop/yarn-site.xml.template

RUN echo export JAVA_HOME=$JAVA_HOME >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo export HADOOP_CONF_DIR=$HADOOP_CONF_DIR >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

RUN mkdir -p /hdfs/namenode && mkdir -p /hdfs/datanode && mkdir $HADOOP_HOME/logs

RUN chmod 700 ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && chmod 700 ${HADOOP_HOME}/sbin/start-dfs.sh
RUN sed s/MASTER_HOSTNAME/localhost/ ${HADOOP_HOME}/etc/hadoop/core-site.xml.template > ${HADOOP_HOME}/etc/hadoop/core-site.xml && \
    sed s/MASTER_HOSTNAME/localhost/ ${HADOOP_HOME}/etc/hadoop/yarn-site.xml.template > ${HADOOP_HOME}/etc/hadoop/yarn-site.xml

ENV HADOOP_FIRST_STARTUP /hdfs/startup.tmp
RUN touch $HADOOP_FIRST_STARTUP

# adampro
COPY adampro.conf.template ${ADAM_HOME}/adampro.conf.template

# hdfs ports
EXPOSE 50010 50020 50070 50075 50090
# mapred ports
EXPOSE 19888
# yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088
# other ports
EXPOSE 49707 2122

# bootstrap
COPY bootstrap.sh ${ADAM_HOME}/bootstrap.sh
RUN chmod 700 ${ADAM_HOME}/bootstrap.sh
