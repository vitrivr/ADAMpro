package org.vitrivr.adampro.storage.engine

import java.net.InetAddress

import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.datatypes.AttributeTypes._
import org.vitrivr.adampro.data.entity.AttributeDefinition
import org.vitrivr.adampro.data.entity.Entity.EntityName
import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.vitrivr.adampro.query.query.Predicate
import org.vitrivr.adampro.utils.Logging
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.{CassandraConnector, PasswordAuthConf}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.vitrivr.adampro.process.SharedComponentContext

import scala.util.{Failure, Random, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class CassandraEngine(private val url: String, private val port: Int, private val user: String, private val password: String, protected val keyspace: String = "public")(@transient override implicit val ac: SharedComponentContext) extends Engine()(ac) with Logging with Serializable {
  private val conn = CassandraConnector(hosts = Set(InetAddress.getByName(url)), port = port, authConf = PasswordAuthConf(user, password))
  private val connectionId =  Random.alphanumeric.take(10).mkString

  ac.sqlContext.setConf(connectionId + "/" + "spark.cassandra.connection.host", url)
  ac.sqlContext.setConf(connectionId + "/" + "spark.cassandra.connection.port", port.toString)
  ac.sqlContext.setConf(connectionId + "/" + "spark.cassandra.connection.username", user)
  ac.sqlContext.setConf(connectionId + "/" + "spark.cassandra.connection.password", password)

  override val name = "cassandra"

  override def supports = Seq(AttributeTypes.AUTOTYPE, AttributeTypes.INTTYPE, AttributeTypes.LONGTYPE, AttributeTypes.FLOATTYPE, AttributeTypes.DOUBLETYPE, AttributeTypes.BOOLEANTYPE, AttributeTypes.STRINGTYPE, AttributeTypes.VECTORTYPE)

  override def specializes = Seq(AttributeTypes.VECTORTYPE)

  override val repartitionable = false

  /**
    *
    * @param props
    */
  def this(props: Map[String, String])(implicit ac: SharedComponentContext) {
    this(props.get("url").get, props.get("port").get.toInt, props.get("user").get, props.get("password").get, props.getOrElse("keyspace", "public"))(ac)
  }


  /**
    *
    */
  def init(): Unit = {
    val keyspaceRes = conn.withClusterDo(_.getMetadata).getKeyspace(keyspace)

    if (keyspaceRes == null) {
      conn.withSessionDo { session =>
        createKeyspace(session)
      }
    }
  }

  init()

  private def createKeyspaceCql(keyspacename: String = keyspace) =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $keyspacename
       |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
       |AND durable_writes = false
       |""".stripMargin

  private def dropKeyspaceCql(keyspacename: String) =
    s"""
       |DROP KEYSPACE IF EXISTS $keyspacename
       |""".stripMargin

  private def createTableCql(tablename: EntityName, schema: String) =
    s"""
       |CREATE TABLE IF NOT EXISTS $tablename
       | ($schema)
       |""".stripMargin

  private def dropTableCql(tablename: EntityName) =
    s"""
       |DROP TABLE IF EXISTS $tablename
       |""".stripMargin


  private def createKeyspace(session: Session, name: String = keyspace): Unit = {
    session.execute(dropKeyspaceCql(name))
    session.execute(createKeyspaceCql(name))
  }


  /**
    * Create the entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes attributes of the entity (w.r.t. handler)
    * @param params     creation parameters
    * @return options to store
    */
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    try {
      val attributeString = attributes.map(attribute => {
        val name = attribute.name
        val cqlType = getCQLType(attribute.attributeType)
        val pk = if (attribute.pk) {
          "PRIMARY KEY"
        } else {
          ""
        }

        s"$name $cqlType $pk".trim
      }).mkString(", ")

      conn.withSessionDo { session =>
        session.execute("use " + keyspace)
        session.execute(createTableCql(storename, attributeString))
      }

      Success(Map())
    } catch {
      case e: Exception =>
        log.error("fatal error when creating bucket in cassandra", e)
        Failure(e)
    }
  }

  /**
    *
    * @param attributetype
    * @return
    */
  private def getCQLType(attributetype: AttributeType): String = attributetype match {
    case INTTYPE => "INT"
    case LONGTYPE => "BIGINT"
    case FLOATTYPE => "FLOAT"
    case DOUBLETYPE => "DOUBLE"
    case BOOLEANTYPE => "BOOLEAN"
    case STRINGTYPE => "TEXT"
    case VECTORTYPE => "LIST<FLOAT>"
    case _ => throw new GeneralAdamException("attribute type " + attributetype.name + " is not supported in cassandra handler")
  }

  /**
    * Check if entity exists.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def exists(storename: String)(implicit ac: SharedComponentContext): Try[Boolean] = {
    try {
      var exists = false
      conn.withSessionDo { session =>
        val tableMeta = session.getCluster.getMetadata.getKeyspace(keyspace).getTable(storename)

        if (tableMeta != null) {
          exists = true
        }
      }
      Success(exists)
    } catch {
      case e: Exception =>
        log.error("fatal error when checking for existence in cassandra", e)
        Failure(e)
    }
  }

  /**
    * Read entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes the attributes to read
    * @param predicates filtering predicates (only applied if possible)
    * @param params     reading parameters
    * @return
    */
  override def read(storename: String, attributes: Seq[AttributeDefinition], predicates: Seq[Predicate], params: Map[String, String])(implicit ac: SharedComponentContext): Try[DataFrame] = {
    try {
      val df = ac.sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> storename, "keyspace" -> keyspace, "cluster" -> connectionId))
        .load()

      var data = df

      predicates.foreach{ predicate =>
        val valueList = predicate.values.map(value => value match {
          case _ : String => "'" + value + "'"
          case _ => value.toString
        })

        data = data.where(predicate.attribute + " " + predicate.operator.getOrElse(" IN ") + " " + valueList.mkString("(", ",", ")"))
      }

      Success(data)
    } catch {
      case e: Exception =>
        log.error("fatal error when reading from cassandra", e)
        Failure(e)
    }
  }

  /**
    * Write entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param df         data
    * @param attributes attributes to store
    * @param mode       save mode (append, overwrite, ...)
    * @param params     writing parameters
    * @return new options to store
    */
  override def write(storename: String, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode = SaveMode.Append, params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    try {
      /*if (mode != SaveMode.Append) {
        throw new UnsupportedOperationException("only appending is supported")
      }*/

      df.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> storename, "keyspace" -> keyspace, "cluster" -> connectionId))
        .save()

      Success(Map())
    } catch {
      case e: Exception =>
        log.error("fatal error when writing to cassandra", e)
        Failure(e)
    }
  }

  /**
    * Drop the entity.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  def drop(storename: String)(implicit ac: SharedComponentContext): Try[Void] = {
    try {
      conn.withSessionDo { session =>
        session.execute("use " + keyspace)
        session.execute(dropTableCql(storename))
      }
      Success(null)
    } catch {
      case e: Exception =>
        log.error("fatal error when dropping from in cassandra", e)
        Failure(e)
    }
  }
}
