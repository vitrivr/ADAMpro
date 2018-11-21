#!/bin/bash

# check that the script is being run as root
if [[ $EUID > 0 ]]; then # we can compare directly with this syntax.
  echo "Please run as root/sudo"
  exit 1
fi

cd $ADAMPRO_CODE

# update the code
git fetch
git checkout $ADAMPRO_BRANCH

UPSTREAM=${1:-'@{u}'}
LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse "$UPSTREAM")
BASE=$(git merge-base @ "$UPSTREAM")

if [ $LOCAL = $REMOTE ]; then
    echo "Up-to-date"

elif [ $LOCAL = $BASE ]; then
    echo "Newer version in repository: Updating repository and re-building"

    git pull
    git submodule update --recursive --remote
    sbt proto
    sbt assembly
    sbt web/assembly
    rm -f ${ADAMPRO_HOME}/log4j.properties
    rm -f ${ADAMPRO_HOME}/ADAMpro-assembly-0.1.0.jar ${ADAMPRO_HOME}/ADAMpro-web-assembly-0.1.0.jar
    cp $ADAMPRO_CODE/conf/log4j.properties ${ADAMPRO_HOME}/log4j.properties
    cp $ADAMPRO_CODE/target/scala-2.11/ADAMpro-assembly-0.1.0.jar ${ADAMPRO_HOME}/ADAMpro-assembly-0.1.0.jar && cp $ADAMPRO_CODE/web/target/scala-2.11/ADAMpro-web-assembly-0.1.0.jar ${ADAMPRO_HOME}/ADAMpro-web-assembly-0.1.0.jar

    reboot
elif [ $REMOTE = $BASE ]; then
    echo "Need to push: Something is wrong"

else
    echo "Diverged: Something is wrong"

fi
