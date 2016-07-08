#!/usr/bin/env bash

##Author: Silvan Heller
## Usage: setup.sh master or setup.sh slave


echo "update & upgrade"
sudo apt-get clean
sudo apt-get -qq update
sudo apt-get -y -qq upgrade
sudo apt-get clean

PKG_OK=$(dpkg-query -W --showformat='${Status}\n' docker-engine |grep "install ok installed")
echo Checking for docker: $PKG_OK
if [ "" == "$PKG_OK" ]
 then
    echo "No somelib. Setting up somelib."
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

PKG_OK=$(dpkg-query -W --showformat='${Status}\n' git|grep "install ok installed")
echo Checking for git: $PKG_OK
if [ "" == "$PKG_OK" ]
 then
    echo "installing git"
    sudo apt-get -y -qq install git
    echo "git installed"
fi

#PKG_OK=$(dpkg-query -W --showformat='${Status}\n' virtualbox-5.0|grep "install ok installed")
#echo Checking for virtualbox: $PKG_OK
#if [ "" == "$PKG_OK" ]
# then
#    echo "No virtualbox. Setting up virtualbox."
#    echo "deb http://download.virtualbox.org/virtualbox/debian xenial contrib" >> /etc/apt/sources.list
#    wget -q https://www.virtualbox.org/download/oracle_vbox_2016.asc -O- | sudo apt-key add -
#    wget -q https://www.virtualbox.org/download/oracle_vbox.asc -O- | sudo apt-key add -
#
#    sudo apt-get -qq update
#    sudo apt-get -y -qq install virtualbox-5.0
#    echo "vbox installed"
#fi
#
#if [ ! -f /usr/local/bin/docker-machine ]; then
#    echo "docker-machine not installed"
#    curl -L https://github.com/docker/machine/releases/download/v0.7.0/docker-machine-`uname -s`-`uname -m` > /usr/local/bin/docker-machine && \
#    chmod +x /usr/local/bin/docker-machine
#    echo "docker-machine installed"
#    #Here you should verify if default exists
#    #Disabled at the moment since VBox needs BIOS-Things
#    #sudo docker-machine create --driver virtualbox default
#fi

sudo docker build -t adampar/spark-base:1.6.2-hadoop2.6 docker-spark/base/

ROLE=$1
if [ $ROLE == "" ]; then
    ROLE="slave"
fi


if [ $ROLE == "master" ]; then
    sudo docker stop spark-master
    sudo docker rm spark-master

    sudo docker rmi adampar/spark-master:1.6.2-hadoop2.6
    sudo docker build -t adampar/spark-master:1.6.2-hadoop2.6 docker-spark/master

    sudo docker run -d -v /home/ubuntu/target:/target --net=host -p 7077:7077 -p 8080:8080 -p 6066:6066 -p 4040:4040 -p 8088:8088 -p 8042:8042 -p 5890:5890 --name spark-master -h spark-master -e ENABLE_INIT_DAEMON=false adampar/spark-master:1.6.2-hadoop2.6

    sudo docker rm -f spark-submit
    sudo docker rmi adampar/spark-submit:1.6.2-hadoop2.6

    #TODO Relative to execution maybe
    sudo docker build -t adampar/spark-submit:1.6.2-hadoop2.6 docker-spark/submit

    sudo docker run -d -v /home/ubuntu/target:/target --net=host --name spark-submit -h spark-submit -e ENABLE_INIT_DAEMON=false adampar/spark-submit:1.6.2-hadoop2.6
fi

if [ $ROLE == "slave" ]; then
    sudo docker stop spark-worker
    sudo docker rm spark-worker
    sudo docker build -t adampar/spark-worker:1.6.2-hadoop2.6 docker-spark/worker

    sudo docker run -d --net=host -p 8081:8081 --name spark-worker -h spark-worker -e ENABLE_INIT_DAEMON=false adampar/spark-worker:1.6.2-hadoop2.6
fi