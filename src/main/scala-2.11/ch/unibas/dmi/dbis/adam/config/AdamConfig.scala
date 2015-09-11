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
  val dataPath = basePath / "data"
  val indexPath = basePath / "index"
  val catalogPath = basePath / "catalog"

  val partitions = 4
}
