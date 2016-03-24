package ch.unibas.dmi.dbis.adam.storage.engine

import java.io.File

import ch.unibas.dmi.dbis.adam.config.{AdamConfig, FieldNames}
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.IndexStorage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object ParquetIndexStorage extends IndexStorage {
  val log = Logger.getLogger(getClass.getName)

  val storage = if (AdamConfig.isBaseOnHadoop) {
    log.debug("storing index on Hadoop")
    new HadoopStorage()
  } else {
    log.debug("storing index locally")
    new LocalStorage()
  }

  override def read(indexName: IndexName, filter: Option[collection.Set[TupleID]]): DataFrame = {
    log.debug("reading index from harddisk")
    storage.read(indexName, filter)
  }
  override def drop(indexName: IndexName): Boolean = {
    log.debug("dropping index from harddisk")
    storage.drop(indexName)
  }
  override def write(indexName: IndexName, index: DataFrame): Boolean = {
    log.debug("writing index to harddisk")
    storage.write(indexName, index)
  }
}


trait GenericIndexStorage extends IndexStorage {
  override def read(indexname: IndexName, filter: Option[scala.collection.Set[TupleID]] = None): DataFrame = {
    val df = SparkStartup.sqlContext.read.parquet(AdamConfig.indexPath + "/" + indexname + ".parquet")

    if(filter.isDefined){
      df.filter(df(FieldNames.idColumnName) isin (filter.toSeq : _*))
    } else {
      df
    }
  }

  override def write(indexname: IndexName, df: DataFrame): Boolean = {
    df.write.mode(SaveMode.Overwrite).parquet(AdamConfig.indexPath + "/" + indexname + ".parquet")
    true
  }
}

/**
  *
  */
class HadoopStorage extends GenericIndexStorage {
  val hadoopConf = new Configuration()

  hadoopConf.set("fs.defaultFS", AdamConfig.basePath)

  if (!FileSystem.get(new Path("/").toUri, hadoopConf).exists(new Path(AdamConfig.indexPath))) {
    FileSystem.get(new Path("/").toUri, hadoopConf).mkdirs(new Path(AdamConfig.indexPath))
  }

  override def drop(indexname: IndexName): Boolean = {
    val path = AdamConfig.indexPath + "/" + indexname + ".parquet"
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", AdamConfig.basePath)
    val hdfs = FileSystem.get(new Path(AdamConfig.indexPath).toUri, hadoopConf)
    hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
    true

  }
}

/**
  *
  */
class LocalStorage extends GenericIndexStorage {
  val indexFolder = new File(AdamConfig.indexPath)

  if (!indexFolder.exists()) {
    indexFolder.mkdirs
  }


  override def drop(indexname: IndexName): Boolean = {
    new File(AdamConfig.indexPath + "/" + indexname + ".parquet").delete()
    true
  }
}
