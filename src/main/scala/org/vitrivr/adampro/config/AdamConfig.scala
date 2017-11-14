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

  val optimizerPath = internalsPath + "/" + "optimizers"
  new File(optimizerPath).mkdirs()

  val optimizerAlgorithm = if (config.hasPath("adampro.optimizer")) {
    Some(config.getString("adampro.optimizer"))
  } else {
    None
  }

  import scala.collection.JavaConversions._

  val engines = config.getStringList("adampro.engines").toIterator.toIndexedSeq

  val grpcPort = config.getInt("adampro.grpc.port")

  var evaluation =  if (config.hasPath("adampro.evaluation")) {
    config.getBoolean("adampro.evaluation")
  } else {
    false
  }

  val maximumTimeToWaitInTraining = 1000 //in seconds

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

  val defaultNumberOfPartitions = if (config.hasPath("adampro.defaultPartitions")) {
    config.getInt("adampro.defaultPartitions")
  } else {
    8
  }

  val defaultNumberOfPartitionsIndex = if (config.hasPath("adampro.defaultPartitionsIndex")) {
    config.getInt("adampro.defaultPartitionsIndex")
  } else {
    4
  }


  val localNodes = if (config.hasPath("adampro.localNodes")) {
    Option(config.getInt("adampro.localNodes"))
  } else {
    None
  }

  val logQueryExecutionTime = if(config.hasPath("adampro.logQueryExecutionTime")){
    config.getBoolean("adampro.logQueryExecutionTime")
  } else {
    false
  }


  val approximateFiltering = if(config.hasPath("adampro.approximateFiltering")){
    config.getBoolean("adampro.approximateFiltering")
  } else {
    false
  }

  val manualPredicatePushdown = if(config.hasPath("adampro.manualPredicatePushdown")){
    config.getBoolean("adampro.manualPredicatePushdown")
  } else {
    false
  }

  object FilteringMethod extends Enumeration {
    val BloomFilter, IsInFilter, SemiJoin = Value
  }


  val filteringMethod = if(config.hasPath("adampro.filteringMethod")){
    config.getString("adampro.filteringMethod").toLowerCase match {
      case "bloomfilter" => FilteringMethod.BloomFilter
      case "isinfilter" => FilteringMethod.IsInFilter
      case "semijoin" => FilteringMethod.SemiJoin
      case _ => FilteringMethod.SemiJoin
    }
  } else {
    FilteringMethod.SemiJoin
  }

  val vaGlobalRefinement = if(config.hasPath("adampro.VAGlobalRefinement")){
    config.getBoolean("adampro.VAGlobalRefinement")
  } else {
    false
  }


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

  /**
    * Cleans paths, e.g. replaces ~ by path to home folder
    *
    * @param s
    * @return
    */
  def cleanPath(s: String): String = AdamConfig.cleanPath(s)
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
