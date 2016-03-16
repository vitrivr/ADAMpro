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
  config.checkValid(ConfigFactory.defaultReference(), "adampro")

  val indexBase = config.getString("adampro.hadoopUrl")
  val hadoopUrl = config.getString("adampro.hadoopUrl")

  val dataPath = config.getString("adampro.dataPath")

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

  val maximumCacheSizeIndex = 10
  val expireAfterAccess = 10 //in minutes

}
