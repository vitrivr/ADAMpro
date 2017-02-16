FROM vitrivr/adampro:latest

ARG MASTER_NODE
ENV MASTER_HOSTNAME $MASTER_NODE
ENV ADAMPRO_MASTER spark://$MASTER_HOSTNAME:7077

COPY adampro.conf.template ${ADAM_HOME}/adampro.conf.template

COPY bootstrap.sh ${ADAM_HOME}/bootstrap.sh
RUN chmod 700 ${ADAM_HOME}/bootstrap.sh

ENTRYPOINT ["/adampro/bootstrap.sh"]

# per default, the node is started as worker
CMD ["--workernode"]