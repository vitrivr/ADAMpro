package ch.unibas.dmi.dbis.adam.storage.engine

import java.util.Properties

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.MetadataStorage
import org.apache.spark.sql.jdbc.AdamDialectRegistrar
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object PostgresMetadataStorage extends MetadataStorage {
  val url = AdamConfig.jdbcUrl

  AdamDialectRegistrar.register(url)

  //TODO: create schema and index on id
  override def create(entityname: EntityName, fields: Option[Map[String, String]]): Unit = ???

  /**
   *
   * @param tablename
   * @return
   */
  override def read(tablename: EntityName): DataFrame = {
    SparkStartup.sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename, "user" -> AdamConfig.jdbcUser, "password" -> AdamConfig.jdbcPassword)
    ).load()
  }

  /**
   *
   * @param tablename
   * @param df
   * @param mode
   */
  override def write(tablename: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append): Boolean = {
    val props = new Properties()
    props.put("user", AdamConfig.jdbcUser)
    props.put("password", AdamConfig.jdbcPassword)
    df.write.mode(mode).jdbc(url, tablename, props)
    true
  }

  /**
   *
   * @param tablename
   */
  override def drop(tablename: EntityName): Boolean = {
    SparkStartup.sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename, "user" -> AdamConfig.jdbcUser, "password" -> AdamConfig.jdbcPassword)
    ).load()
    true
  }
}
