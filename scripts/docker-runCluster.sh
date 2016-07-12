#!/usr/bin/env bash

export SPARK_MASTER=10.34.58.136

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"
echo "starting bash-script"

cp -R $DIR/conf $DIR/target/
rm $DIR/target/conf/application.conf

sudo docker stop spark-submit
sudo docker start spark-submit
