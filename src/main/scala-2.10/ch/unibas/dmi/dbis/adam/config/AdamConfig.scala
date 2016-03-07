package ch.unibas.dmi.dbis.adam.config
import com.typesafe.config.ConfigFactory

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object AdamConfig{
  val config = ConfigFactory.load()
  config.checkValid(ConfigFactory.defaultReference(), "adamtwo")

  //TODO: move to external config file
  val hadoopUrl = "hdfs://127.0.0.1:9000" //default: 9000

  val dataBase = "/Users/gianiv01/tmp"
  val indexBase = hadoopUrl

  val dataPath = dataBase + "/" + "data"
  val indexPath = "/Users/gianiv01/tmp/hdfs/adamtwo/index"
  val catalogPath = dataBase + "/" + "catalog"
  val indexMetaCatalogPath = catalogPath + "/" + "indexmeta"
  val evaluationPath = dataBase + "/" + "evaluation"

  val jdbcUrl = "jdbc:postgresql://127.0.0.1:5432/docker" //default: 5432
  val jdbcUser = "docker"
  val jdbcPassword = "docker"

  val cassandraUrl = "127.0.0.1"
  val cassandraPort = "9042" //default: 9042
  val cassandraUsername = "cassandra"
  val cassandraPassword = "cassandra"

  val grpcPort = 5890

  val partitions = 4

  var evaluation = false
}
