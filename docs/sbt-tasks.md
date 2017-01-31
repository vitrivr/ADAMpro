sbt tasks
====================

We provide various sbt tasks to be run on the ADAMpro project, to simplify the deployment and development. For instance, go to the ADAMpro folder and run `sbt proto` to generate the protobuf file, or run `sbt buildDocker` to build a runnable docker image which contains ADAMpro.

*   `proto`: generates a jar file from the grpc folder and includes it in the main project (this is necessary, as shadowing is necessary of the netty dependency)

*   `setupDocker`: sets up a self-contained Docker container for running ADAMpro
