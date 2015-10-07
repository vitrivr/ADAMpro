package ch.unibas.dmi.dbis.adam.storage.engine

import java.io.File

import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.main.{SparkStartup, Startup}
import ch.unibas.dmi.dbis.adam.storage.components.{IndexStorage, TableStorage}
import ch.unibas.dmi.dbis.adam.table.{DefaultTable, Table}
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, DataFrame}
import com.databricks.spark.avro._


/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object AvroDataStorage extends TableStorage with IndexStorage {
  val config = Startup.config

  /**
   *
   * @param tablename
   * @return
   */
  override def readTable(tablename: TableName): Table = {
    DefaultTable(tablename, SparkStartup.sqlContext.read.avro(config.dataPath + "/" + tablename + ".avro"))
  }

  /**
   *
   * @param tablename
   * @param df
   * @param mode
   */
  override def writeTable(tablename : TableName, df: DataFrame, mode : SaveMode = SaveMode.Append): Unit = {
    df.write.mode(mode).avro(config.dataPath + "/" + tablename + ".avro")
  }

  /**
   *
   * @param indexname
   * @return
   */
  override def readIndex(indexname: IndexName) : DataFrame = {
    SparkStartup.sqlContext.read.avro(config.indexPath + "/" + indexname + ".avro")
  }


  /**
   *
   * @param indexname
   * @param df
   */
  override def writeIndex(indexname: IndexName, df: DataFrame): Unit = {
    df.write.mode(SaveMode.Overwrite).avro(config.indexPath + "/" + indexname + ".avro")
  }

  /**
   *
   * @param tablename
   */
  override def dropTable(tablename: TableName): Unit = {
    FileUtils.deleteQuietly(new File(config.dataPath + "/" + tablename + ".avro"))
  }

  /**
   *
   * @param indexname
   */
  override def dropIndex(indexname: IndexName): Unit = {
    FileUtils.deleteQuietly(new File(config.indexPath + "/" + indexname + ".avro"))
  }
}
