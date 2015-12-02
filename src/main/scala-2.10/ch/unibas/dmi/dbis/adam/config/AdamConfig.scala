package ch.unibas.dmi.dbis.adam.config
import com.typesafe.config.{Config, ConfigFactory}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class AdamConfig(config : Config) {
  //TODO: move to external config file
  config.checkValid(ConfigFactory.defaultReference(), "adamtwo")

  private val hadoopBase = "hdfs://HadoopMaster:54310/spark/adamtwo"
  private val localBase = "/datadrive/adamtwo"

  val hivePath = hadoopBase + "/" +  "hive"
  val dataPath = localBase + "/" + "data"
  val indexPath = hadoopBase + "/" + "index"
  val catalogPath = localBase + "/" + "catalog"
  val indexMetaCatalogPath = catalogPath + "/" + "indexmeta"
  val evaluationPath = localBase + "/" + "evaluation"

  val jdbcUrl = "jdbc:postgresql://192.168.99.101:6543/postgres"
  val jdbcUser = "postgres"
  val jdbcPassword = "postgres"

  val restHost = "0.0.0.0"
  val restPort = 5890

  val partitions = 4
}
