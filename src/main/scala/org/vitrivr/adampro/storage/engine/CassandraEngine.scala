package org.vitrivr.adampro.storage.engine

import java.net.InetAddress

import org.vitrivr.adampro.datatypes.FieldTypes
import org.vitrivr.adampro.datatypes.FieldTypes._
import org.vitrivr.adampro.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import org.vitrivr.adampro.entity.AttributeDefinition
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.query.Predicate
import org.vitrivr.adampro.utils.Logging
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.{CassandraConnector, PasswordAuthConf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Random, Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class CassandraEngine(private val url: String, private val port: Int, private val user: String, private val password: String, protected val keyspace: String = "public")(@transient override implicit val ac: AdamContext) extends Engine()(ac) with Logging with Serializable {
  private val conn = CassandraConnector(hosts = Set(InetAddress.getByName(url)), port = port, authConf = PasswordAuthConf(user, password))
  private val connectionId =  Random.alphanumeric.take(10).mkString

  ac.sqlContext.setConf(connectionId + "/" + "spark.cassandra.connection.host", url)
  ac.sqlContext.setConf(connectionId + "/" + "spark.cassandra.connection.port", port.toString)
  ac.sqlContext.setConf(connectionId + "/" + "spark.cassandra.connection.username", user)
  ac.sqlContext.setConf(connectionId + "/" + "spark.cassandra.connection.password", password)

  override val name = "cassandra"

  override def supports = Seq(FieldTypes.AUTOTYPE, FieldTypes.SERIALTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.STRINGTYPE, FieldTypes.FEATURETYPE)

  override def specializes = Seq(FieldTypes.FEATURETYPE)

  override val repartitionable = false

  /**
    *
    * @param props
    */
  def this(props: Map[String, String])(implicit ac: AdamContext) {
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
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: AdamContext): Try[Map[String, String]] = {
    try {
      val attributeString = attributes.map(attribute => {
        val name = attribute.name
        val cqlType = getCQLType(attribute.fieldtype)
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
    * @param fieldtype
    * @return
    */
  private def getCQLType(fieldtype: FieldType): String = fieldtype match {
    case INTTYPE => "INT"
    case AUTOTYPE => "BIGINT"
    case LONGTYPE => "BIGINT"
    case FLOATTYPE => "FLOAT"
    case DOUBLETYPE => "DOUBLE"
    case STRINGTYPE => "TEXT"
    case BOOLEANTYPE => "BOOLEAN"
    case FEATURETYPE => "LIST<FLOAT>"
    case _ => throw new GeneralAdamException("field type " + fieldtype.name + " is not supported in cassandra handler")
  }

  /**
    * Check if entity exists.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def exists(storename: String)(implicit ac: AdamContext): Try[Boolean] = {
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
  override def read(storename: String, attributes: Seq[AttributeDefinition], predicates: Seq[Predicate], params: Map[String, String])(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      import org.apache.spark.sql.functions.udf
      val castToFeature = udf((c: Seq[Float]) => {
        new FeatureVectorWrapper(c)
      })

      val df = ac.sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> storename, "keyspace" -> keyspace, "cluster" -> connectionId))
        .load()
      //TODO: possibly use predicate

      var data = df

      df.schema.fields.filter(_.dataType.isInstanceOf[ArrayType]).foreach { field =>
        data = data.withColumn(field.name, castToFeature(col(field.name)))
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
  override def write(storename: String, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode = SaveMode.Append, params: Map[String, String])(implicit ac: AdamContext): Try[Map[String, String]] = {
    try {
      /*if (mode != SaveMode.Append) {
        throw new UnsupportedOperationException("only appending is supported")
      }*/
      
      var data = df
      import org.apache.spark.sql.functions.{col, udf}
      val castToSeq = udf((c: FeatureVectorWrapper) => {
        c.toSeq
      })
      df.schema.fields.filter(_.dataType.isInstanceOf[FeatureVectorWrapperUDT]).foreach { field =>
        data = data.withColumn(field.name, castToSeq(col(field.name)))
      }

      data.write
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
  def drop(storename: String)(implicit ac: AdamContext): Try[Void] = {
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
