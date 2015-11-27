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

  private val base = Some(config.getString("adamtwo.basePath")).getOrElse("data/")
  val basePath : File = File(base)
  val hivePath = basePath / "hive"
  val dataPath = basePath / "data"
  val indexPath = basePath / "index"
  val catalogPath = basePath / "catalog"

  val jdbcUrl = "jdbc:postgresql://192.168.99.101:6543/postgres"
  val jdbcUser = "postgres"
  val jdbcPassword = "postgres"

  val solrUrl = "localhost:8983"

  val restHost = "localhost"
  val restPort = 8888

  val partitions = 4
}
