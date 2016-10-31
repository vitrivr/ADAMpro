#!/usr/bin/env bash

########################################
### Install docker-engine, git, update & upgrade everything
########################################


##TODO Modify so it works on other ubuntus than 16.04
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