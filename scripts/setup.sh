#!/usr/bin/env bash

##Author: Silvan Heller

## TODO Find better handling than getopts which only takes one char
## TODO support multiple roles in one script
## TODO port based on MASTER_PORT in the run -p flags

################## USAGE ###########################################
## -u installs docker and git
## -b rebuilds the spark-base image
## -r is either WORKER or MASTER or SUBMIT
## -s and -h are in format ip:port and are used for the spark master and the hadoop namenode. use :7077 and :9000 please :)

## This script assumes that in the folder where scripts/ is placed, you want to have a folder target/ where the conf-folder and the jar is going to be


##########################
# Cleaning existing images
##########################
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

echo "Parsing hadoop- and spark-vars"

#############################################
## Extract vars before doing anything
#############################################
while getopts ':h:s:r:b' option; do
  case $option in
    s)
      echo "-spark was triggered, Parameter: $OPTARG" >&2
      export SPARK_MASTER=$OPTARG
      ;;
    h)
      echo "-hadoop was triggered, Parameter: $OPTARG" >&2
      export HADOOP_NAMENODE=$OPTARG
      ;;

    r)
      echo "-role was triggered, Parameter: $OPTARG" >&2
      export ROLE=$OPTARG
      ;;
    b)
      echo "-base was triggered"
        export BASE="yes"
      ;;
    u)
      echo "Installing docker and git, updating etc."
      sudo bash $DIR/install.sh
      ;;
    :)
      echo "Error, Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

#####################
# Extract IP and Port
#####################
IFS=':' read -r -a array <<< "$SPARK_MASTER"
SPARK_MASTER_IP=${array[0]}
SPARK_MASTER_PORT=${array[1]}

IFS=':' read -r -a array <<< "$HADOOP_NAMENODE"
HADOOP_MASTER_IP=${array[0]}
HADOOP_MASTER_PORT=${array[1]}

echo "Building Containers"

if [[ $BASE == "yes" ]]; then
    echo "rebuilding base container"
    sudo docker stop spark-master
    sudo docker rm spark-master
    sudo docker rmi adampar/spark-master:1.6.2-hadoop2.6

    sudo docker stop spark-worker
    sudo docker rm spark-worker
    sudo docker rmi adampar/spark-worker:1.6.2-hadoop2.6

    sudo docker stop spark-submit
    sudo docker rm spark-submit
    sudo docker rmi adampar/spark-submit:1.6.2-hadoop2.6

    sudo docker rmi adampar/spark-base:1.6.2-hadoop2.6
    sudo docker build -t adampar/spark-base:1.6.2-hadoop2.6 $DIR/scripts/docker-spark/base/
fi

## Builds Containers. The actual method call is below
buildContainer() {
    echo "Building Container: $1"
    ROLE=$1

    if [ $ROLE == "MASTER" ]; then
        sudo docker stop spark-master
        sudo docker rm spark-master
        sudo docker rmi adampar/spark-master:1.6.2-hadoop2.6

        #postgresql
        sudo docker stop postgresql
        sudo docker rm postgresql
        sudo docker run --net=host -p 5432:5432 -h postgresql --name postgresql -d orchardup/postgresql:latest

        # adampro
        sudo docker build -t adampar/spark-master:1.6.2-hadoop2.6 $DIR/scripts/docker-spark/master

        ##9000 is for HDFS
        # 8080, 7077 and 6066 are to listen to spark-submit / spark GUI
        # #8088, 8042 are legacy from the old code TODO are those necessary?
        #5890 is the grpc-port TODO Needed here?
        sudo docker run -d -e "SPARK_MASTER_PORT=$SPARK_MASTER_PORT" \
         -e "SPARK_MASTER_IP=$SPARK_MASTER_IP" \
         -e "HADOOP_NAMENODE=$HADOOP_NAMENODE" \
         -p 8080:8080 -p $HADOOP_MASTER_PORT:$HADOOP_MASTER_PORT -p $SPARK_MASTER_PORT:$SPARK_MASTER_PORT -p 6066:6066 -p 8088:8088 -p 8042:8042 \
         --net=host --hostname spark --name spark-master \
         adampar/spark-master:1.6.2-hadoop2.6
    fi

    if [ $ROLE == "WORKER" ]; then
        sudo docker stop spark-worker
        sudo docker rm spark-worker
        sudo docker rmi adampar/spark-worker:1.6.2-hadoop2.6

        sudo docker build -t adampar/spark-worker:1.6.2-hadoop2.6 $DIR/scripts/docker-spark/worker

        #Ports: 8081 is the UI, 9000 the HDFS Port -> TODO Scale this based on Input
        # 7077, 6066 are for jobs, 4040 for the Job UI
        # 8088 and 8042 are legacy ports TODO
        sudo docker run -d \
         -e "SPARK_MASTER=spark://$SPARK_MASTER" \
         -e "HADOOP_NAMENODE=$HADOOP_NAMENODE" \
         -p 8081:8081 -p $HADOOP_MASTER_PORT:$HADOOP_MASTER_PORT -p $SPARK_MASTER_PORT:$SPARK_MASTER_PORT -p 6066:6066 -p 4040:4040 -p 8088:8088 -p 8042:8042 \
         --net=host --name spark-worker -h spark-worker \
         adampar/spark-worker:1.6.2-hadoop2.6
    fi

    if [ $ROLE == "SUBMIT" ]; then
        sudo docker stop spark-submit
        sudo docker rm spark-submit
        sudo docker rmi adampar/spark-submit:1.6.2-hadoop2.6

        sudo docker build -t adampar/spark-submit:1.6.2-hadoop2.6 $DIR/scripts/docker-spark/submit

        # TODO Try which ports are necessary
        # TODO Make spark-submit great again. Multiple things:
        # 1) extract from submit.sh to an exec-thing - that allows the dev to control deploy-mode and such in script
        # 5890 is the grpc-Port if you run the .jar on the submit-container
        # 50543, 8087, 8089 and 47957 are needed to communicate with the workers
        # 4040 is the Apllication UI
        sudo docker run -d -v $DIR/target:/target -v $DIR/target/conf/log4j.properties:/usr/local/spark/conf/log4j.properties \
        -e "HADOOP_NAMENODE=$HADOOP_NAMENODE" \
         -p 50543:50543 -p 8087:8087 -p 47957:47957 -p 4040:4040  -p 8089:8089 -p 5890:5890 \
         --net=host --name spark-submit -h spark-submit \
         adampar/spark-submit:1.6.2-hadoop2.6
    fi

}


buildContainer $ROLE