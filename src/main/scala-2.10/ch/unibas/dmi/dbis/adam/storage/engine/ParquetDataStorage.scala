package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.IndexStorage
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object ParquetDataStorage extends IndexStorage {
  override def read(indexname: IndexName) : DataFrame = SparkStartup.sqlContext.read.parquet(AdamConfig.indexPath + "/" + indexname)
  override def write(indexname: IndexName, df: DataFrame): Unit = df.write.mode(SaveMode.Overwrite).parquet(AdamConfig.indexPath + "/" + indexname)
  override def drop(indexname: IndexName): Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(AdamConfig.hadoopBase), hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(AdamConfig.indexPath + "/" + indexname), true) } catch { case _ : Throwable => { } }
  }
}
