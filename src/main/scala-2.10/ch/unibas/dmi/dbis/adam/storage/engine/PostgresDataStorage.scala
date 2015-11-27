package ch.unibas.dmi.dbis.adam.storage.engine

import java.util.Properties

import ch.unibas.dmi.dbis.adam.main.{SparkStartup, Startup}
import ch.unibas.dmi.dbis.adam.storage.components.MetadataStorage
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import org.apache.spark.sql.jdbc.AdamDialectRegistrar
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object PostgresDataStorage extends MetadataStorage {
  val config = Startup.config
  val url = config.jdbcUrl

  AdamDialectRegistrar.register(url)

  /**
   *
   * @param tablename
   * @return
   */
  override def read(tablename: EntityName): DataFrame = {
    SparkStartup.sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename, "user" -> config.jdbcUser, "password" -> config.jdbcPassword)
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
    props.put("user", config.jdbcUser)
    props.put("password", config.jdbcPassword)
    df.write.mode(mode).jdbc(url, tablename, props)
  }

  /**
   *
   * @param tablename
   */
  override def drop(tablename: EntityName): Unit = {
    SparkStartup.sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename, "user" -> config.jdbcUser, "password" -> config.jdbcPassword)
    ).load()
  }
}
