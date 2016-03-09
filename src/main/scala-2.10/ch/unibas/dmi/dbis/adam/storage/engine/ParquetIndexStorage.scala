package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.Tuple._
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
object ParquetIndexStorage extends IndexStorage {
  //TODO: refactor
  val hadoopConf = new Configuration()
  hadoopConf.set("fs.defaultFS", AdamConfig.hadoopUrl)


  if(!FileSystem.get(new Path("/").toUri, hadoopConf).exists(new Path(AdamConfig.indexPath))){
    FileSystem.get(new Path("/").toUri, hadoopConf).mkdirs(new Path(AdamConfig.indexPath))
  }

  override def read(indexname: IndexName, filter: Option[scala.collection.Set[TupleID]] = None) : DataFrame = {
    //TODO: implement UnboundRecordFilter
    //see https://adambard.com/blog/parquet-protobufs-spark/ and http://zenfractal.com/2013/08/21/a-powerful-big-data-trio/
    SparkStartup.sqlContext.read.parquet(AdamConfig.indexBase + "/" + AdamConfig.indexPath + "/" + indexname + ".parquet")
  }

  override def write(indexname: IndexName, df: DataFrame): Boolean = {
    println(AdamConfig.indexBase + "/" + AdamConfig.indexPath + "/" + indexname + ".parquet")
    df.write.mode(SaveMode.Overwrite).parquet(AdamConfig.indexBase + "/" + AdamConfig.indexPath + "/" + indexname + ".parquet")
    true
  }

  override def drop(indexname: IndexName): Boolean = {
    val path = AdamConfig.indexPath + "/" + indexname + ".parquet"

    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", AdamConfig.hadoopUrl)
    val hdfs = FileSystem.get(new Path(AdamConfig.indexPath).toUri, hadoopConf)
    hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
    true
  }
}



