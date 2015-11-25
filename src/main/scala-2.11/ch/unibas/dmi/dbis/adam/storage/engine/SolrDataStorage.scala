package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.{Startup, SparkStartup}
import ch.unibas.dmi.dbis.adam.storage.components.MetadataStorage
import org.apache.spark.sql.{SaveMode, DataFrame}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object SolrDataStorage extends MetadataStorage {
  val config = Startup.config
  val url = config.jdbcUrl

  /**
   *
   * @param tablename
   * @return
   */
  override def read(tablename: EntityName): DataFrame = {
    SparkStartup.sqlContext.read.format("solr")
      .option("zkhost", config.solrUrl)
      .option("collection",tablename)
      .load()
  }

  /**
   *
   * @param tablename
   */
  override def drop(tablename: EntityName): Unit = ???

  /**
   *
   * @param tablename
   * @param df
   * @param mode
   */
  override def write(tablename: EntityName, df: DataFrame, mode: SaveMode): Unit = ???
}
