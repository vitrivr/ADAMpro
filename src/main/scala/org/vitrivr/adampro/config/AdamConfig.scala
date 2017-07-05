package org.vitrivr.adampro.config

import java.io.File

import org.vitrivr.adampro.utils.Logging
import com.typesafe.config.{Config, ConfigFactory}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class AdamConfig extends Serializable with Logging {
  val config = {
    val externalConfig = getExternalConfigFile().map(_.resolve())

    val internalConfig = if (!ConfigFactory.load().hasPath("adampro")) {
      //this is somewhat a hack to have different configurations depending on whether we have an assembly-jar or we
      //run the application "locally"
      ConfigFactory.load("assembly.conf")
    } else {
      ConfigFactory.load()
    }.resolve()

    if (externalConfig.isDefined) {
      externalConfig.get.resolve()
    } else {
      internalConfig
    }
  }
  log.trace(config.toString)
  config.checkValid(ConfigFactory.defaultReference(), "adampro")

  val internalsPath = AdamConfig.cleanPath(config.getString("adampro.internalsPath"))
  val schedulerFile = internalsPath + "/" + "scheduler.xml"

  import scala.collection.JavaConversions._

  val engines = config.getStringList("adampro.engines").toIterator.toIndexedSeq

  val grpcPort = config.getInt("adampro.grpc.port")

  var evaluation = false

  val loglevel = config.getString("adampro.loglevel")

  val maximumCacheSizeEntity = 1000
  val expireAfterAccessEntity = 60 //in minutes

  val maximumCacheSizeIndex = 1000
  val expireAfterAccessIndex = 60 //in minutes

  val maximumCacheSizeBooleanQuery = 1000
  val expireAfterAccessBooleanQuery = 60 //in minutes

  val maximumCacheSizeQueryResults = 1000
  val expireAfterAccessQueryResults = 60 //in minutes

  val master = if (config.hasPath("adampro.master")) {
    Option(config.getString("adampro.master"))
  } else {
    None
  }

  val defaultNumberOfPartitions = 8

  val defaultNumberOfPartitionsIndex = 4


  val localNodes = if (config.hasPath("adampro.localNodes")) {
    Option(config.getInt("adampro.localNodes"))
  } else {
    None
  }

  val logQueryExecutionTime = true

  /**
    *
    * @param path
    * @return
    */
  def getString(path: String) = config.getString(path)

  /**
    *
    * @param engine
    * @return
    */
  def getStorageProperties(engine: String) = config.getObject("storage." + engine).toMap.mapValues(_.unwrapped().toString)

  /**
    * Reads external config file.
    *
    * @param name
    * @return
    */
  private def getExternalConfigFile(name: String = "adampro.conf"): Option[Config] = {
    var path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath()
    path = path.substring(0, path.lastIndexOf("/"))
    path = path.replaceAll("%20", " ")

    val file = new File(path, name)

    if (file.exists()) {
      Some(ConfigFactory.parseFile(file))
    } else {
      None
    }
  }
}

object AdamConfig {
  /**
    * Cleans paths, e.g. replaces ~ by path to home folder
    *
    * @param s
    * @return
    */
  def cleanPath(s: String): String = {
    var newString = s

    if (newString.startsWith("~" + File.separator)) {
      newString = System.getProperty("user.home") + newString.substring(1);
    }

    newString
  }
}
