package ch.unibas.dmi.dbis.adam.config
import com.typesafe.config.ConfigFactory

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object AdamConfig{
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

  val indexPath = config.getString("adampro.indexPath")

  val catalogPath = config.getString("adampro.catalogPath")
  val indexMetaCatalogPath = catalogPath + "/" + "indexmeta"

  val evaluationPath = config.getString("adampro.evaluationPath")

  val jdbcUrl =  config.getString("adampro.jdbc.url")
  val jdbcUser = config.getString("adampro.jdbc.user")
  val jdbcPassword = config.getString("adampro.jdbc.password")

  val cassandraUrl = config.getString("adampro.cassandra.url")
  val cassandraPort = config.getString("adampro.cassandra.port")
  val cassandraUsername = config.getString("adampro.cassandra.user")
  val cassandraPassword = config.getString("adampro.cassandra.password")
  val cassandraKeyspace = "adampro"

  val grpcPort = config.getInt("adampro.grpc.port")

  var evaluation = false

  val loglevel = config.getString("adampro.loglevel")

  val maximumCacheSizeIndex = 10
  val expireAfterAccess = 10 //in minutes

  val master = if(config.hasPath("adampro.master")){
    Option(config.getString("adampro.master"))
  } else {
    None
  }
}
