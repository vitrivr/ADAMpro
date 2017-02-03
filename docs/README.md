# ADAMpro documentation

ADAMpro is the persistent polystore (based on Apache Spark) for all data required for retrieval. 

## Getting started

### sbt setupDocker
ADAMpro can be built and run using the provided [scripts](../scripts). The easiest way to run ADAMpro is to use [Docker](https://www.docker.com/).

Locally check out the git repository and run

```
sbt setupDocker
```

in the main folder. Note that `sbt setupDocker` is an alias for running the `./scripts/setupDocker.sh` script.

The script will build the Docker image (the Docker file is located in the folder `./scripts/Docker/`), possibly populate it with data (if data is available in the correct format in the folder `./scripts/Docker/data/`), and run it. After the creation of the container, you can navigate to

```
http://localhost:4040
```

to open the ADAMpro UI. Furthermore, you can connect on port 5890 to make use of the database.

Note that the Docker container downloads the newest version of the code from the repository and builds new jars every time the container is started. By adjusting the `/adampro/build.sh` script, this behaviour can be turned off.

### Manually building the Docker image
Navigate to the Docker file in `./scripts/Docker/` and run 

```
docker build -t adampro .
```

Note again that data that is located in the sub-folder `data` is copied into the Docker image and is being used (if in the correct format). 
 
Then run 

```
docker run -d -p 5890:5890 -p 9099:9099 adampro
```

to start the Docker container.


### pre-built Images
We provide pre-built Docker images to download:

- [Empty ADAMpro](http://download-dbis.dmi.unibas.ch/ADAMpro/adampro2-nodata.tar)
- [ADAMpro with OSVC](http://download-dbis.dmi.unibas.ch/ADAMpro/adampro2-osvc.tar)

To use the pre-built images, download the image and run

```
docker load < adampro2-nodata.tar
```

### Running ADAMpro locally
ADAMpro can be built using [sbt](http://www.scala-sbt.org/). We provide various sbt tasks to be run on the ADAMpro project, to simplify the deployment and development.

* `assembly` creates a fat jar with ADAMpro to submit to spark, run `sbt proto` first
* `proto` generates a jar file from the grpc folder and includes it in the main project (this is necessary, as shadowing is necessary of the netty dependency)
* `setupDocker` sets up a self-contained Docker image for running ADAMpro

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

ADAMpro can also be started locally, e.g., from an IDE. For this, remove the `% "provided"` statements from `build.sbt` and run the main class `org.vitrivr.adampro.main.Startup. 


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

### Debugging
We recommend the use of IntelliJ IDEA for developing ADAMpro. It can be run locally using the run commands in the IDE for debugging purposes.

Note that the behaviour of ADAMpro, when run locally, might be different than when submitted to Apache Spark (using `./spark-submit`), in particular because of the inclusion of different package versions (e.g., Apache Spark will come with a certain version of netty, which is used even if `build.sbt` includes a newer version; we refrain from using the `spark.driver.userClassPathFirst` option as this is experimental).

ADAMpro can be debugged even if being submitted to Spark. By setting the debugging option in the `SPARK_SUBMIT_OPTS` command before submitting, a remote debugger can be attached:
```
export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
```
In here, we have opened port 5005 and have the application wait until a debugger attaches. We use port 5005 in the Docker containers provided for debugging (however, the `suspend` option is not turned on in the Docker container).

For more information on this consider e.g., this article: https://community.hortonworks.com/articles/15030/spark-remote-debugging.html

### Unit tests
ADAMpro comes with a set of unit tests which can be run from the [test package](https://github.com/vitrivr/ADAMpro/tree/master/src/test). Note that for having all test pass, a certain setup is necessary. For instance, for having the PostGIS test pass, the database has to be set up and it must be configured in the configuration file. You may use the script `setupLocalUnitTests.sh` for setting up all the necessary Docker containers for then performing the unit tests.

### Flame graphs
For checking the performance of ADAMpro, also consider the creation of flame graphs. For more information see [here](https://gist.github.com/kayousterhout/7008a8ebf2babeedc7ce6f8723fd1bf4).