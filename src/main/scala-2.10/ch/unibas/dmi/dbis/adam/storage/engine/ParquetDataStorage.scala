package ch.unibas.dmi.dbis.adam.storage.engine

import java.io.File

import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.{SparkStartup, Startup}
import ch.unibas.dmi.dbis.adam.storage.components.IndexStorage
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object ParquetDataStorage extends IndexStorage {
  val config = Startup.config

  override def read(indexname: IndexName) : DataFrame = SparkStartup.sqlContext.read.parquet(config.indexPath + "/" + indexname)
  override def write(indexname: IndexName, df: DataFrame): Unit = df.write.mode(SaveMode.Overwrite).parquet(config.indexPath + "/" + indexname)
  override def drop(indexname: IndexName): Unit = FileUtils.deleteQuietly(new File(config.indexPath + "/" + indexname))
}
