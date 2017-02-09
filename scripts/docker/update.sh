#!/bin/bash

# check that the script is being run as root
if [[ $EUID > 0 ]]; then # we can compare directly with this syntax.
  echo "Please run as root/sudo"
  exit 1
fi

cd $ADAM_CODE

# update the code
git checkout $ADAMPRO_BRANCH
git fetch

UPSTREAM=${1:-'@{u}'}
LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse "$UPSTREAM")
BASE=$(git merge-base @ "$UPSTREAM")

if [ $LOCAL = $REMOTE ]; then
    echo "Up-to-date"

elif [ $LOCAL = $BASE ]; then
    echo "Newer version in repository: Updating repository and re-building"

    git pull
    sbt proto
    sbt assembly
    sbt web/assembly
    rm -f ${ADAM_HOME}/log4j.xml
    rm -f ${ADAM_HOME}/ADAMpro-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-web-assembly-0.1.0.jar
    cp $ADAM_CODE/conf/log4j.xml ${ADAM_HOME}/log4j.xml
    cp $ADAM_CODE/target/scala-2.11/ADAMpro-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-assembly-0.1.0.jar && cp $ADAM_CODE/web/target/scala-2.11/ADAMpro-web-assembly-0.1.0.jar ${ADAM_HOME}/ADAMpro-web-assembly-0.1.0.jar

    # reboot so that the new code is used
    # su --login - postgres --command "$POSTGRES_HOME/bin/pg_ctl -w stop -D $PGDATA"
    # solr stop -noprompt
    # reboot
elif [ $REMOTE = $BASE ]; then
    echo "Need to push: Something is wrong"

else
    echo "Diverged: Something is wrong"

fi
