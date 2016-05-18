package ch.unibas.dmi.dbis.adam.config
import com.typesafe.config.ConfigFactory

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object AdamConfig extends Serializable {
  val config = {
    val defaultConfig = ConfigFactory.load()

    if(!defaultConfig.hasPath("adampro")){
      //this is somewhat a hack to have different configurations depending on whether we have an assembly-jar or we
      //run the application "locally"
      ConfigFactory.load("assembly.conf")
    } else {
      defaultConfig
    }
  }
  config.checkValid(ConfigFactory.defaultReference(), "adampro")

  val basePath = config.getString("adampro.basePath")
  val isBaseOnHadoop = basePath.startsWith("hdfs")

  val dataPath = config.getString("adampro.dataPath")
  val indexPath = config.getString("adampro.indexPath")

  val catalogPath = config.getString("adampro.catalogPath")
  val indexMetaCatalogPath = catalogPath + "/" + "indexmeta"

  val internalsPath = config.getString("adampro.internalsPath")
  val schedulerFile = internalsPath + "/" + "scheduler.xml"

  val jdbcUrl =  config.getString("adampro.jdbc.url")
  val jdbcUser = config.getString("adampro.jdbc.user")
  val jdbcPassword = config.getString("adampro.jdbc.password")

  val grpcPort = config.getInt("adampro.grpc.port")

  var evaluation = false

  val loglevel = config.getString("adampro.loglevel")

  val maximumCacheSizeEntity = 10
  val expireAfterAccessEntity = 10 //in minutes

  val maximumCacheSizeIndex = 10
  val expireAfterAccessIndex = 10 //in minutes

  val maximumCacheSizeBooleanQuery = 100
  val expireAfterAccessBooleanQuery = 1 //in minutes

  val maximumCacheSizeQueryResults = 100
  val expireAfterAccessQueryResults = 30 //in minutes

  val master = if(config.hasPath("adampro.master")){
    Option(config.getString("adampro.master"))
  } else {
    None
  }

  val defaultNumberOfPartitions = 8

  val localNodes = if(config.hasPath("adampro.localNodes")){
    Option(config.getInt("adampro.localNodes"))
  } else {
    None
  }
}
