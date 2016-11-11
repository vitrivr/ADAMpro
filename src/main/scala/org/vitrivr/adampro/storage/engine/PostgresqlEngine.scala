package org.vitrivr.adampro.storage.engine

import java.sql.Connection
import java.util.Properties

import org.vitrivr.adampro.datatypes.FieldTypes
import org.vitrivr.adampro.datatypes.FieldTypes.FieldType
import org.vitrivr.adampro.entity.AttributeDefinition
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.query.Predicate
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
class PostgresqlEngine(private val url: String, private val user: String, private val password: String, private val schema: String = "public") extends Engine with Serializable {
  //TODO: check if changing schema breaks tests!

  override val name = "postgresql"

  override def supports: Seq[FieldType] = Seq(FieldTypes.AUTOTYPE, FieldTypes.SERIALTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.BOOLEANTYPE)

  override def specializes: Seq[FieldType] = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.BOOLEANTYPE)

  /**
    *
    * @param props
    */
  def this(props: Map[String, String]) {
    this(props.get("url").get, props.get("user").get, props.get("password").get, props.getOrElse("schema", "public"))
  }

  private val ds = new ComboPooledDataSource
  ds.setDriverClass("org.postgresql.Driver")
  ds.setJdbcUrl(url)
  ds.setProperties(props)

  init()

  /**
    * Opens a connection to the PostgreSQL database.
    *
    * @return
    */
  protected def openConnection(): Connection = {
    ds.getConnection
  }

  protected def init() {
    val connection = openConnection()

    try {
      val createSchemaStmt = s"""CREATE SCHEMA IF NOT EXISTS $schema;""".stripMargin

      connection.createStatement().executeUpdate(createSchemaStmt)
    } catch {
      case e: Exception =>
        log.error("fatal error when setting up relational engine", e)
    } finally {
      connection.close()
    }
  }

  lazy val props = {
    val props = new Properties()
    props.put("url", url)
    props.put("user", user)
    props.put("password", password)
    props.put("driver", "org.postgresql.Driver")
    props.put("currentSchema", schema)
    props
  }

  lazy val propsMap = props.keySet().toArray.map(key => key.toString -> props.get(key).toString).toMap

  /**
    * Create the entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes attributes of the entity (w.r.t. handler)
    * @param params     creation parameters
    * @return options to store
    */
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: AdamContext): Try[Map[String, String]] = {
    log.debug("postgresql create operation")
    val connection = openConnection()

    try {
      val structFields = attributes.map {
        field => StructField(field.name, field.fieldtype.datatype)
      }.toSeq

      val df = ac.sqlContext.createDataFrame(ac.sc.emptyRDD[Row], StructType(structFields))

      val tableStmt = write(storename, df, attributes, SaveMode.ErrorIfExists, params)

      if (tableStmt.isFailure) {
        return Failure(tableStmt.failed.get)
      }

      //make fields unique
      val uniqueStmt = attributes.filter(_.params.getOrElse("unique", "false").toBoolean).map {
        field =>
          val fieldname = field.name
          s"""ALTER TABLE $storename ADD UNIQUE ($fieldname)""".stripMargin
      }.mkString("; ")

      //add index to table
      val indexedStmt = attributes.filter(_.params.getOrElse("indexed", "false").toBoolean).map {
        field =>
          val fieldname = field.name
          s"""CREATE INDEX ON $storename ($fieldname)""".stripMargin
      }.mkString("; ")

      //add primary key
      val pkfield = attributes.filter(_.pk)
      assert(pkfield.size <= 1)
      val pkStmt = pkfield.map {
        case field =>
          val fieldname = field.name
          s"""ALTER TABLE $storename ADD PRIMARY KEY ($fieldname)""".stripMargin
      }.mkString("; ")

      connection.createStatement().executeUpdate(uniqueStmt + "; " + indexedStmt + "; " + pkStmt)

      Success(Map())
    } catch {
      case e: Exception =>
        Failure(e)
    } finally {
      connection.close()
    }
  }


  /**
    * Check if entity exists.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def exists(storename: String)(implicit ac: AdamContext): Try[Boolean] = {
    log.debug("postgresql exists operation")
    val connection = openConnection()

    try {
      val existsSql = s"""SELECT EXISTS ( SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = '$storename')""".stripMargin
      val results = connection.createStatement().executeQuery(existsSql)
      results.next()
      Success(results.getBoolean(1))
    } catch {
      case e: Exception =>
        Failure(e)
    } finally {
      connection.close()
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
    log.debug("postgresql read operation")

    try {
      //TODO: possibly adjust in here for partitioning
      var df = if (predicates.nonEmpty) {
        //TODO: remove equality in predicates?
        ac.sqlContext.read.jdbc(url, storename, predicates.map(_.sqlString).toArray, props)
      } else {
        ac.sqlContext.read.jdbc(url, storename, props)
      }

      attributes.foreach { attribute =>
        df = df.withColumn(attribute.name, df.col(attribute.name).cast(attribute.fieldtype.datatype))
      }

      Success(df)
    } catch {
      case e: Exception =>
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
    log.debug("postgresql write operation")

    try {
      df.write.mode(mode).jdbc(url, storename, props)
      Success(Map())
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  /**
    * Drop the entity.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def drop(storename: String)(implicit ac: AdamContext): Try[Void] = {
    log.debug("postgresql drop operation")
    val connection = openConnection()

    try {
      val dropTableSql = s"""DROP TABLE $storename""".stripMargin
      connection.createStatement().executeUpdate(dropTableSql)
      Success(null)
    } catch {
      case e: Exception =>
        log.error("error in dropping table in postgresql", e)
        Failure(e)
    } finally {
      connection.close()
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: PostgresqlEngine => that.url.equals(url) && that.user.equals(user) && that.password.equals(password)
      case _ => false
    }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + url.hashCode
    result = prime * result + user.hashCode
    result = prime * result + password.hashCode
    result
  }
}