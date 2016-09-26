sbt tasks
====================

We provide various sbt tasks to be run on the ADAMpro project, to simplify the deployment and development. For instance, go to the ADAMpro folder and run `sbt proto` to generate the protobuf file, or run `sbt buildDocker` to build a runnable docker image which contains ADAMpro.

*   `proto`: generates a jar file from the grpc folder and includes it in the main project (this is necessary, as shadowing is necessary of the netty dependency)
*   `setupDocker`: sets up the necessary docker containers for running ADAMpro using docker
*   `destroyDocker`: deletes the docker containers
*   `startDocker`: starts the docker containers
*   `stopDocker`: stops the docker containers without deleting them
*   `runDocker`: runs ADAMpro in docker container, performs first an assembly of and then does a sparkSubmit
*   `buildDocker`: build a self-contained docker ADAMpro container (adampro:latest) with all necessary packages installed