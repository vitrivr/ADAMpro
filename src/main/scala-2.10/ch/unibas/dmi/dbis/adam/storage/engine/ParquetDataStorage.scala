package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.IndexStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object ParquetDataStorage extends IndexStorage {

  val hadoopConf = new Configuration()
  hadoopConf.set("fs.defaultFS", AdamConfig.hadoopUrl)
  val path = new Path(AdamConfig.hadoopBase)

  if(!FileSystem.get(new Path("/").toUri, hadoopConf).exists(path)){
    FileSystem.get(new Path("/").toUri, hadoopConf).mkdirs(path)
  }

  val hdfs = FileSystem.get(path.toUri, hadoopConf)

  override def read(indexname: IndexName) : DataFrame = SparkStartup.sqlContext.read.parquet(AdamConfig.hadoopUrl + "/" + AdamConfig.indexPath + "/" + indexname)
  override def write(indexname: IndexName, df: DataFrame): Unit = df.write.mode(SaveMode.Overwrite).parquet(AdamConfig.hadoopUrl + "/" + AdamConfig.indexPath + "/" + indexname)
  override def drop(indexname: IndexName): Unit = {
    try { hdfs.delete(new org.apache.hadoop.fs.Path(AdamConfig.indexPath + "/" + indexname), true) } catch { case _ : Throwable => { } }
  }
}
