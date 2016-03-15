package ch.unibas.dmi.dbis.adam.storage.engine

import java.sql.{Connection, DriverManager}
import java.util.Properties

import ch.unibas.dmi.dbis.adam.config.{FieldNames, AdamConfig}
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.MetadataStorage
import org.apache.spark.sql.jdbc.AdamDialectRegistrar
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object PostgresMetadataStorage extends MetadataStorage {
  val url = AdamConfig.jdbcUrl
  AdamDialectRegistrar.register(url)

  /**
    * Opens a connection to the PostgreSQL database.
    *
    * @return
    */
  private def openConnection(): Connection ={
    Class.forName("org.postgresql.Driver").newInstance
    DriverManager.getConnection(AdamConfig.jdbcUrl, AdamConfig.jdbcUser, AdamConfig.jdbcPassword)
  }

  override def create(tablename: EntityName, fields: Map[String, DataType]): Boolean = {
    val structFields = fields.map{
      case (name, fieldtype) => StructField(name, fieldtype)
    }.toSeq

    val df = SparkStartup.sqlContext.createDataFrame(SparkStartup.sc.emptyRDD[Row], StructType(structFields))

    write(tablename, df, SaveMode.ErrorIfExists)

    true
  }

  override def read(tablename: EntityName): DataFrame = {
    SparkStartup.sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename, "user" -> AdamConfig.jdbcUser, "password" -> AdamConfig.jdbcPassword)
    ).load()
  }

  override def write(tablename: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append): Boolean = {
    val props = new Properties()
    props.put("user", AdamConfig.jdbcUser)
    props.put("password", AdamConfig.jdbcPassword)
    df.write.mode(mode).jdbc(url, tablename, props)
    true
  }

  override def drop(tablename: EntityName): Boolean = {
    SparkStartup.sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename, "user" -> AdamConfig.jdbcUser, "password" -> AdamConfig.jdbcPassword)
    ).load()
    true
  }
}
