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

  val indexBase = config.getString("adamtwo.hadoopUrl")
  val hadoopUrl = config.getString("adamtwo.hadoopUrl")

  val dataPath = config.getString("adamtwo.dataPath")

  val indexPath = "/Users/gianiv01/tmp/hdfs/adamtwo/index"
  val catalogPath = config.getString("adamtwo.catalogPath")
  val indexMetaCatalogPath = catalogPath + "/" + "indexmeta"

  val evaluationPath = config.getString("adamtwo.evaluationPath")

  val jdbcUrl =  config.getString("adamtwo.jdbc.url")
  val jdbcUser = config.getString("adamtwo.jdbc.user")
  val jdbcPassword = config.getString("adamtwo.jdbc.password")

  val cassandraUrl = config.getString("adamtwo.cassandra.url")
  val cassandraPort = config.getString("adamtwo.cassandra.port")
  val cassandraUsername = config.getString("adamtwo.cassandra.user")
  val cassandraPassword = config.getString("adamtwo.cassandra.password")
  val cassandraKeyspace = "adamtwo"

  val grpcPort = config.getInt("adamtwo.grpc.port")

  var evaluation = false

  val maximumCacheSizeIndex = 10
  val expireAfterAccess = 10 //in minutes

}
