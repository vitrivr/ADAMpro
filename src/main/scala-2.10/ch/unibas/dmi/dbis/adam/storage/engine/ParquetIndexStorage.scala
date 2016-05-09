package ch.unibas.dmi.dbis.adam.storage.engine

import java.io.File

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.components.IndexStorage
import org.apache.commons.io.FileUtils
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

  val storage : GenericIndexStorage = if (AdamConfig.isBaseOnHadoop) {
    log.debug("storing index on Hadoop")
    new HadoopStorage()
  } else {
    log.debug("storing index locally")
    new LocalStorage()
  }

  override def read(indexName: IndexName)(implicit ac: AdamContext): DataFrame = {
    log.debug("reading index from harddisk")
    storage.read(indexName)
  }

  override def drop(indexName: IndexName)(implicit ac: AdamContext): Boolean = {
    log.debug("dropping index from harddisk")
    storage.drop(indexName)
  }

  override def write(indexName: IndexName, index: DataFrame)(implicit ac: AdamContext): Boolean = {
    log.debug("writing index to harddisk")
    storage.write(indexName, index)
  }

  override def exists(indexname: IndexName): Boolean = storage.exists(indexname)

  private[engine] trait GenericIndexStorage extends IndexStorage {
    override def read(indexname: IndexName)(implicit ac: AdamContext): DataFrame = {
      ac.sqlContext.read.parquet(AdamConfig.indexPath + "/" + indexname + ".parquet")
    }

    override def write(indexname: IndexName, df: DataFrame)(implicit ac: AdamContext): Boolean = {
      df
        .write.mode(SaveMode.Overwrite).parquet(AdamConfig.indexPath + "/" + indexname + ".parquet")
      true
    }
  }

  /**
    *
    */
  private class HadoopStorage extends GenericIndexStorage {
    val hadoopConf = new Configuration()

    hadoopConf.set("fs.defaultFS", AdamConfig.basePath)

    if (!FileSystem.get(new Path("/").toUri, hadoopConf).exists(new Path(AdamConfig.indexPath))) {
      FileSystem.get(new Path("/").toUri, hadoopConf).mkdirs(new Path(AdamConfig.indexPath))
    }

    override def drop(indexname: IndexName)(implicit ac: AdamContext): Boolean = {
      val path = AdamConfig.indexPath + "/" + indexname + ".parquet"
      val hadoopConf = new Configuration()
      hadoopConf.set("fs.defaultFS", AdamConfig.basePath)
      val hdfs = FileSystem.get(new Path(AdamConfig.indexPath).toUri, hadoopConf)

      hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
      true

    }

    /**
      *
      * @param indexname
      * @return
      */
    override def exists(indexname: IndexName): Boolean = {
      val path = AdamConfig.indexPath + "/" + indexname + ".parquet"
      FileSystem.get(new Path(AdamConfig.indexPath).toUri, hadoopConf).exists(new org.apache.hadoop.fs.Path(path))
    }
  }

  /**
    *
    */
  private class LocalStorage extends GenericIndexStorage {
    val indexFolder = new File(AdamConfig.indexPath)

    if (!indexFolder.exists()) {
      indexFolder.mkdirs
    }


    override def drop(indexname: IndexName)(implicit ac: AdamContext): Boolean = {
      FileUtils.deleteDirectory(new File(AdamConfig.indexPath + "/" + indexname + ".parquet"))
      true
    }

    /**
      *
      * @param indexname
      * @return
      */
    override def exists(indexname: IndexName): Boolean = {
      new File(AdamConfig.indexPath + "/" + indexname + ".parquet").exists()
    }
  }

}


