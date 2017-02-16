# ADAMpro documentation

ADAMpro is the persistent polystore (based on Apache Spark) for all data required for retrieval. 

## Getting started

### Docker container

A Docker image of ADAMpro is released on [Docker Hub](https://hub.docker.com/r/vitrivr/adampro/).

Pull the image using
```
docker pull vitrivr/adampro
```

or run directly using (with the recommended ports being opened):
```
docker run --name adampro -p 5005:5005 -p 5890:5890 -p 9099:9099 -p 5432:5432 -p 9000:9000 -p 4040:4040 -d vitrivr/adampro
```

After the creation of the container, you can navigate to

```
http://localhost:4040
```
to open the ADAMpro UI. Furthermore, you can connect on port 5890 to make use of the database.

We provide the OSVC data for [download](http://download-dbis.dmi.unibas.ch/ADAMpro/osvc.tar.gz).

Untar the folder and copy to `adampro/data`; then restart the container.

```
docker cp osvc.tar.gz adampro:/adampro/data
docker exec adampro tar -C /adampro/data/ -xzf /adampro/data/osvc.tar.gz 
docker restart adampro
```

Our Docker containers come with an update script located at `/adampro/update.sh`, which allows you to check out the newest version of the code from the repository and re-build the jars without creating a new container (and therefore loosing existing data). To run the update routine, run in your host system:

```
docker exec adampro /adampro/update.sh
```

### Running ADAMpro locally
ADAMpro can be built using [sbt](http://www.scala-sbt.org/). We provide various sbt tasks to simplify the deployment and development.

* `assembly` creates a fat jar with ADAMpro to submit to spark, run `sbt proto` first
* `proto` generates a jar file from the grpc folder and includes it in the main project (this is necessary, as shadowing is necessary of the netty dependency)

Other helpful sbt tasks include
* `dependencyTree` displays the dependencies to other packages
* `stats` to show code statistics

Because of its project structure, for building ADAMpro, you have to first run

```
sbt proto
```

which generates the proto-files and creates a jar file containing the proto sources into the `./lib/` folder.

Running

```
sbt assembly
```

(and `sbt web/assembly` for the ADAMpro UI), a jar file is created which can then be submitted to Apache Spark using

```
./spark-submit --master "local[4]" --driver-memory 2g --executor-memory 2g --class org.vitrivr.adampro.main.Startup $ADAM_HOME/ADAMpro-assembly-0.1.0.jar
```

ADAMpro can also be started locally, e.g., from an IDE. For this, remove the `% "provided"` statements from `build.sbt` and the marked line `ExclusionRule("io.netty")`, and run the main class `org.vitrivr.adampro.main.Startup`. You can use
```
sbt run
```
for running ADAMpro, as well. **Note that the storage engines specified in the configuration have to be running already or you have to adjust the config file accordingly.**


## Configuration

### Configuration files
ADAMpro can be configured using a configuration file. This repository contains a `./conf/` folder with configuration files.

- `application.conf` is used when running ADAMpro from an IDE
- `assembly.conf` is the conf file included in the assembly jar (when running `sbt assembly`)

When starting ADAMpro, you can provide a `adampro.conf` file in the same path as the jar, which is then used instead of the default configuration. (Note the file `adampro.conf.template` which is used as a template for the Docker container.)

### Configuration parameters
The configuration file can be used to specify configurations for running ADAMpro. The file [ADAMConfig.scala](https://github.com/vitrivr/ADAMpro/blob/master/src/main/scala/org/vitrivr/adampro/config/AdamConfig.scala) reads the configuration file and provides the configurations to the application.

The file contains information on 
- the log level, e.g., `loglevel = "INFO"`
- the path to all the internal files (catalog, etc.), e.g., `internalsPath =  "/adampro/internals"`
- the grpc port, e.g., `grpc {port = "5890"}`
- the storage engines to use, e.g., `engines = ["parquet", "index", "postgres", "postgis", "cassandra", "solr"]`
 
 For all the storage engines specified, in the `storage` section, more details have to be provided (note that the name specified in `engines` must match the name in the `storage` section):
```
  parquet {
    engine = "ParquetEngine"
    hadoop = true
    basepath = "hdfs://spark:9000/"
    datapath = "/adampro/data/"
  }
```

or 

```
  parquet {
    engine = "ParquetEngine"
    hadoop = false
    path = "~/adampro-tmp/data/"
  }
```

The parameters specified in here are passed directly to the storage engines; it may make sense to consider the code of the single storage engine to see which parameters are necessary to specify (or to consider the exemplary configuration files in the configuration folder). The name of the class is specified in the field `engine`.

## Code basis and Repository
ADAMpro builds on [Apache Spark 2](http://spark.apache.org) and uses a large variety of libraries and packages, e.g. [Google Protocol Buffers](https://developers.google.com/protocol-buffers/) and [grpc](http://www.grpc.io/). The repository has the following structure:

* `chronos` package for evaluating ADAMpro using the Uni Basel Chronos project
* `conf` folder for configuration files; note that the conf folder is automatically included to the resources
* `grpc` the proto file (included from the [proto sub-repository](https://github.com/vitrivr/ADAMpro-Protobuf))
* `grpcclient` general grpc client code for communicating with the grpc server
* `importer` client code for importing proto files to ADAMpro
* `scripts` useful scripts for deploying running ADAMpro
* `src` ADAMpro sources
  * `api` rather static API used within ADAMpro
  * `catalog` storing information on entities, indexes, etc.
  * `config` for retrieving the ADAMpro configuration
  * `datatypes` defines datatypes used throughout ADAMpro; we try to formulate most datatypes as variables, e.g. the datatype of the internally used `TupleID` or the datatype of `feature vectors` is specified here
  * `entity` defines the information for an entity, caches, partitioners
  * `helpers` general helpers
  * `index` defines the information for an index, caches, partitioners and all available index structures in ADAMpro; note that for new index structures you should adhere to the given code structure and naming (i.e., using an `XYZIndex` class and `XYZIndexGenerator`) as index structures are loaded via reflections
  * `main` contains the main classes
  * `ml` contains machine learning classes
  * `query` defines query execution classes, e.g., distance measures, query handlers, caches, progressive querying
  * `rpc` implementation of the rpc protocol
  * `storage` defines the storage engines available together with the registry
  * `utils` general utils classes, mostly experimental/temporary code
* `web` web UI of ADAMpro


## Development

### Unit tests
ADAMpro comes with a set of unit tests which can be run from the [test package](https://github.com/vitrivr/ADAMpro/tree/master/src/test). Note that for having all test pass, a certain setup is necessary. For instance, for having the PostGIS test pass, the database has to be set up and it must be configured in the configuration file. You may use the script `setupLocalUnitTests.sh` for setting up all the necessary Docker containers for then performing the unit tests.

### Debugging
We recommend the use of IntelliJ IDEA for developing ADAMpro. It can be run locally using the run commands in the IDE for debugging purposes.

Note that the behaviour of ADAMpro, when run locally, might be different than when submitted to Apache Spark (using `./spark-submit`), in particular because of the inclusion of different package versions (e.g., Apache Spark will come with a certain version of netty, which is used even if `build.sbt` includes a newer version; we refrain from using the `spark.driver.userClassPathFirst` option as this is experimental).

ADAMpro can be debugged even if being submitted to Spark. By setting the debugging option in the `SPARK_SUBMIT_OPTS` command before submitting, a remote debugger can be attached:
```
export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```
In here, we have opened port 5005 and, given the `suspend` option, have the application wait until a debugger attaches.

The Docker container we provide has the `SPARK_SUBMIT_OPTS` options set and we use port 5005 in the Docker containers provided for debugging (however, note that the `suspend` option which makes the application wait until a debugger attaches is turned off in the Docker container).

In your IDE, bind to the application by setting up remote debugging on the port specified. For more information on how to use remote debugging consider e.g., this article: https://community.hortonworks.com/articles/15030/spark-remote-debugging.html

### Flame graphs
For checking the performance of ADAMpro, also consider the creation of flame graphs. For more information see [here](https://gist.github.com/kayousterhout/7008a8ebf2babeedc7ce6f8723fd1bf4).

## Deployment
For introductory information see the [getting started](#getting-started) section in this documentation.

### Distributed deployment without HDFS
The distributed deployment without HDFS can be used if ADAMpro is being deployed on one single machine only (but still in a simulated distributed environment).

Check out the ADAMpro repository. The folder `scripts/docker-nohdfs` contains a `docker-compose.yml` file which can be used with `docker-compose`. For this, move into the `docker-nohdfs` folder an run:
```
docker-compose up
```
This will start up a master and a single worker node. To add more workers (note that the number of masters is limited to 1), run the `scale` command and specify the number of workers you would like to deploy in total:
```
docker-compose scale worker = 5
```
Note that this setup will not use Hadoop for creating a HDFS, but will rather just mount a folder to all Docker containers (both master and worker container). Therefore this deployment will only work if all containers run on one single machine.


### Distributed deployment with HDFS

#### Using docker-compose
The distributed deployment with HDFS can be used to run ADAMpro with data being distributed over HDFS. This can be helpful for truly distributed setups.

The folder `scripts/docker-hdfs` contains a `docker-compose.yml`; move into the `docker-hdfs` folder an run:
```
docker-compose up
```
This will start up a master and a single worker node. Note that using the `scale` command of `docker-compose` you may create multiple workers; however, the number of master nodes (and Hadoop name nodes) is limited to 1.

#### Using Docker swarm
Consider [the official documentation](https://docs.docker.com/engine/swarm/swarm-tutorial/) and [this inofficial documentation](https://github.com/sfedyakov/hadoop-271-cluster#3-running-hadoop-cluster-on-distributed-machines-with-docker-machine-and-docker-compose) for more information on how to use the images with Docker swarm and how to set up a Docker swarm cluster.