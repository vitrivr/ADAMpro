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
object PostgresDataStorage extends MetadataStorage {
  val url = AdamConfig.jdbcUrl

  AdamDialectRegistrar.register(url)

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
  override def write(tablename: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append): Unit = {
    val props = new Properties()
    props.put("user", AdamConfig.jdbcUser)
    props.put("password", AdamConfig.jdbcPassword)
    df.write.mode(mode).jdbc(url, tablename, props)
  }

  /**
   *
   * @param tablename
   */
  override def drop(tablename: EntityName): Unit = {
    SparkStartup.sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename, "user" -> AdamConfig.jdbcUser, "password" -> AdamConfig.jdbcPassword)
    ).load()
  }
}
