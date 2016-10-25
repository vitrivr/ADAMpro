#!/bin/bash

####################
# builds a self-contained docker ADAMpro (adampro:latest)container with all necessary packages installed.
####################

ADAMPRODIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ".." && pwd )"

cp -R $ADAMPRODIR/target/scala-2.10/ADAMpro-assembly-0.1.0.jar $ADAMPRODIR/scripts/docker-selfcontained
cp -R $ADAMPRODIR/web/target/scala-2.10/ADAMpro-web-assembly-0.1.0.jar $ADAMPRODIR/scripts/docker-selfcontained

docker build -t adampro $ADAMPRODIR/scripts/docker-selfcontained 

echo "Self-contained ADAMpro container has been built."
echo "Run ´docker run -p 5890:5890 -p 9099:9099 -d adampro´ to create and start a ADAMpro container; run ´docker save -o adampro.tar adampro´ to export."
