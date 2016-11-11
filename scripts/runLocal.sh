#!/bin/bash

####################
# runs ADAMpro locally
####################

spark-submit —class "org.vitrivr.adampro.main.Startup" —master "local[4]" —executor-memory 8G —driver-memory 8G ../target/scala-2.10/ADAMpro-assembly-0.1.0.jar