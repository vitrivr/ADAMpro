#!/usr/bin/env bash

##Author: Silvan Heller
## Usage: setup.sh master or setup.sh worker or setup.sh submit

echo "update & upgrade"
sudo apt-get clean
sudo apt-get -qq update
sudo apt-get -y -qq upgrade
sudo apt-get clean
echo "update & upgrade done"

#Installing docker-engine if it doesn't exist
PKG_OK=$(dpkg-query -W --showformat='${Status}\n' docker-engine |grep "install ok installed")
echo Checking for docker: $PKG_OK
if [ "" == "$PKG_OK" ]
 then
    echo "No docker. Setting up docker."
    sudo apt-get -y -qq install apt-transport-https ca-certificates
    sudo apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 58118E89F3A912897C070ADBF76221572C52609D
    sudo rm /etc/apt/sources.list.d/docker.list
    echo "deb https://apt.dockerproject.org/repo ubuntu-xenial main" >> /etc/apt/sources.list.d/docker.list

    sudo apt-get -qq update
    sudo apt-get purge lxc-docker
    sudo apt-cache policy docker-engine

    sudo apt-get -y -qq install linux-image-extra-$(uname -r)
    sudo apt-get -y -qq install docker-engine
    sudo service docker start
    sudo systemctl enable docker
    echo "docker installed"
fi

#Installing git if it doesn't exist
PKG_OK=$(dpkg-query -W --showformat='${Status}\n' git|grep "install ok installed")
echo Checking for git: $PKG_OK
if [ "" == "$PKG_OK" ]
 then
    echo "installing git"
    sudo apt-get -y -qq install git
    echo "git installed"
fi

#Cleaning existing images TODO Add rebuild-enable flag

rm -r target/
mkdir target/

sudo docker stop spark-master
sudo docker rm spark-master
sudo docker rmi adampar/spark-master:1.6.2-hadoop2.6

sudo docker rm -f spark-submit
sudo docker rmi adampar/spark-submit:1.6.2-hadoop2.6

sudo docker stop spark-worker
sudo docker rm spark-worker
sudo docker rmi adampar/spark-worker:1.6.2-hadoop2.6

#This assumes your folder structure contains target/ and scripts/ in the same folder if you are building the spark-submit dinner
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"
sudo docker rmi adampar/spark-base:1.6.2-hadoop2.6
sudo docker build -t adampar/spark-base:1.6.2-hadoop2.6 $DIR/scripts/docker-spark/base/

ROLE=$1
if [ $ROLE == "" ]; then
    return
fi

if [ $ROLE == "master" ]; then
    echo "building master containers"
    sudo docker stop postgresql
    sudo docker rm postgresql
    sudo docker run --net=host -p 5432:5432 -h postgresql --name postgresql -d orchardup/postgresql:latest

    sudo docker build -t adampar/spark-master:1.6.2-hadoop2.6 $DIR/scripts/docker-spark/master

    ##9000 is for HDFS #8080, 7077 and 6066 are to listen to spark-submit / spark GUI ##8088, 8042 are legacy from the old code TODO are those necessary? ##5890 is the grpc-port TODO Needed here?
    sudo docker run -d --net=host -p 8080:8080 -p 9000:9000 -p 7077:7077 -p 6066:6066 -p 8088:8088 -p 8042:8042 --hostname spark --name spark-master adampar/spark-master:1.6.2-hadoop2.6

    echo "building submit containers"
    sudo docker build -t adampar/spark-submit:1.6.2-hadoop2.6 $DIR/scripts/docker-spark/submit
    #Port explanation see submit-container # 5890 is the grpc-port (needed here) # 4040 is for the application UI
    sudo docker run -d -v $DIR/target:/target -v $DIR/target/conf/log4j.properties:/usr/local/spark/conf/log4j.properties -p 50543:50543 -p 8087:8087 -p 47957:47957 -p 4040:4040  -p 8089:8089 -p 5890:5890 --net=host --name spark-submit -h spark-submit adampar/spark-submit:1.6.2-hadoop2.6
fi

if [ $ROLE == "submit" ]; then
    echo "building submit containers"
    sudo docker build -t adampar/spark-submit:1.6.2-hadoop2.6 $DIR/scripts/docker-spark/submit

    #TODO Try if 8089 is necessary
    #Old line sudo docker run -d -e "SPARK_LOCAL_IP=127.0.0.1" -v $DIR/target:/target  -v ~/.ssh/id_rsa:/root/.ssh/id_rsa -v ~/.ssh/id_rsa.pub:/root/.ssh/id_rsa.pub -p 50543:50543 -p 8087:8087 -p 47957:47957 -p 8089:8089 -p 8080:8080 -p 9000:9000 -p 7077:7077 -p 6066:6066 -p 4040:4040 -p 8088:8088 -p 8042:8042 -p 5890:5890 --net=host --name spark-submit -h spark-submit adampar/spark-submit:1.6.2-hadoop2.6
    #Port explanation see submit-container
    sudo docker run -d -v $DIR/target/conf/log4j.properties:/usr/local/spark/conf/log4j.properties -v $DIR/target:/target -p 50543:50543 -p 8087:8087 -p 47957:47957 -p 8089:8089 --net=host --name spark-submit -h spark-submit adampar/spark-submit:1.6.2-hadoop2.6
fi


if [ $ROLE == "worker" ]; then
    sudo docker build -t adampar/spark-worker:1.6.2-hadoop2.6 $DIR/scripts/docker-spark/worker
    #Explanation see spark-master
    sudo docker run -d --net=host -p 8081:8081 -p 9000:9000 -p 7077:7077 -p 6066:6066 -p 4040:4040 -p 8088:8088 -p 8042:8042 --name spark-worker -h spark-worker -e ENABLE_INIT_DAEMON=false adampar/spark-worker:1.6.2-hadoop2.6
fi