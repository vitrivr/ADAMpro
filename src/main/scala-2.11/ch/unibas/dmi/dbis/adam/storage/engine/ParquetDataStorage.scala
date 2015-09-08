package ch.unibas.dmi.dbis.adam.storage.engine

import java.io.File

import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.{SparkStartup, Startup}
import ch.unibas.dmi.dbis.adam.storage.components.{IndexStorage, TableStorage}
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object ParquetDataStorage extends TableStorage with IndexStorage {
  val config = Startup.config

  /**
   *
   * @param tablename
   * @return
   */
  override def readTable(tablename: TableName): Table = {
    Table(tablename, SparkStartup.sqlContext.read.load(config.dataPath + "/" + tablename))
  }

  /**
   *
   * @param tablename
   * @param df
   * @param mode
   */
  override def writeTable(tablename : TableName, df: DataFrame, mode : SaveMode = SaveMode.Append): Unit = {
    df.write.mode(mode).save(config.dataPath + "/" + tablename)
  }

  /**
   *
   * @param indexname
   * @return
   */
  override def readIndex(indexname: IndexName) : DataFrame = {
    SparkStartup.sqlContext.read.load(config.indexPath + "/" + indexname)
  }


  /**
   *
   * @param indexname
   * @param df
   */
  override def writeIndex(indexname: IndexName, df: DataFrame): Unit = {
    df.write.mode(SaveMode.Overwrite).save(config.indexPath + "/" + indexname)
  }

  /**
   *
   * @param tablename
   */
  override def dropTable(tablename: TableName): Unit = {
    FileUtils.deleteQuietly(new File(config.dataPath + "/" + tablename))
  }

  /**
   *
   * @param indexname
   */
  override def dropIndex(indexname: IndexName): Unit = {
    FileUtils.deleteQuietly(new File(config.indexPath + "/" + indexname))
  }
}
