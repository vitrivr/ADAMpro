package ch.unibas.dmi.dbis.adam.config
import com.typesafe.config.{ ConfigFactory, Config }
import scala.reflect.io.File

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class AdamConfig(config : Config) {
  config.checkValid(ConfigFactory.defaultReference(), "adamtwo")

  private val hadoopBase = "hdfs://HadoopMaster:54310/spark/adamtwo"
  private val localBase = "/datadrive/adamtwo"

  val hadoopBasePath : File = File(hadoopBase)
  val localBasePath : File = File(localBase)

  val hivePath = hadoopBasePath / "hive"
  val dataPath = localBasePath / "data"
  val indexPath = hadoopBasePath / "index"
  val catalogPath = localBasePath / "catalog"
  val evaluationPath = localBasePath / "evaluation"

  val jdbcUrl = "jdbc:postgresql://192.168.99.101:6543/postgres"
  val jdbcUser = "postgres"
  val jdbcPassword = "postgres"

  val restHost = "0.0.0.0"
  val restPort = 5890

  val partitions = 4
}
