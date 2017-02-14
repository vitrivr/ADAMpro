FROM vitrivr/adampro:latest

COPY adampro.conf.template ${ADAM_HOME}/adampro.conf.template

COPY worker.sh ${ADAM_HOME}/worker.sh
RUN chmod 700 ${ADAM_HOME}/worker.sh

COPY master.sh ${ADAM_HOME}/master.sh
RUN chmod 700 ${ADAM_HOME}/master.sh