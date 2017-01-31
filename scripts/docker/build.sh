#!/bin/bash

cd $ADAM_CODE

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
    sbt proto && sbt assembly && sbt web/assembly
    rm -f ${ADAM_HOME}/log4j2.xml
    rm -f ${ADAM_HOME}/ADAMpro-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-web-assembly-0.1.0.jar
    cp $ADAM_CODE/conf/log4j2.xml ${ADAM_HOME}/log4j2.xml
    cp $ADAM_CODE/target/scala-2.11/ADAMpro-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-assembly-0.1.0.jar && cp $ADAM_CODE/web/target/scala-2.11/ADAMpro-web-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-web-assembly-0.1.0.jar

elif [ $REMOTE = $BASE ]; then
    echo "Need to push: Something is wrong"

else
    echo "Diverged: Something is wrong"

fi
