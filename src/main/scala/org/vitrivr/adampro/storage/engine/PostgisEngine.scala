package org.vitrivr.adampro.storage.engine

import java.sql.{Connection, DriverManager}

import org.vitrivr.adampro.datatypes.FieldTypes
import org.vitrivr.adampro.datatypes.FieldTypes.FieldType
import org.vitrivr.adampro.entity.AttributeDefinition
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.query.Predicate
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class PostgisEngine(private val url: String, private val user: String, private val password: String) extends PostgresqlEngine(url, user, password, "public") {
  //TODO: gis functions only available in the public schema

  override val name: String = "postgis"

  override def supports: Seq[FieldType] = Seq(FieldTypes.AUTOTYPE, FieldTypes.SERIALTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.STRINGTYPE, FieldTypes.GEOMETRYTYPE, FieldTypes.GEOGRAPHYTYPE)

  override def specializes: Seq[FieldType] = Seq(FieldTypes.GEOMETRYTYPE, FieldTypes.GEOGRAPHYTYPE)

  /**
    *
    * @param props
    */
  def this(props: Map[String, String]) {
    this(props.get("url").get, props.get("user").get, props.get("password").get)
  }

  /**
    * Opens a connection to a PostGIS database.
    *
    * @return
    */
  override protected def openConnection(): Connection = {
    val connection = DriverManager.getConnection(url, props)
    connection.setSchema("public")
    connection.asInstanceOf[org.postgresql.PGConnection].addDataType("geometry", classOf[org.postgis.PGgeometry])
    connection
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

    val query = params.getOrElse("query", "*")
    val limit = params.getOrElse("limit", "ALL")
    val stmt = s"(SELECT $query FROM $storename LIMIT $limit) AS $storename"

    try {
      val predicate = params.get("predicate").map(Seq(_))

      //TODO: possibly adjust in here for partitioning
      val df = if (predicate.isDefined) {
        ac.sqlContext.read.jdbc(url, stmt, predicate.get.toArray, props)
      } else {
        ac.sqlContext.read.jdbc(url, stmt, props)
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
      df.write.mode(mode)
        .format("org.apache.spark.sql.execution.datasources.gis.DataSource")
        .options(propsMap ++ Seq("table" -> storename))
        .save
      Success(Map())
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }
}