package ch.unibas.dmi.dbis.adam.storage.engine

import java.io.File

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.components.IndexStorage
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object ParquetIndexStorage extends IndexStorage {

  val storage: GenericIndexStorage = if (AdamConfig.isBaseOnHadoop) {
    log.info("storing index on Hadoop")
    new HadoopStorage()
  } else {
    log.info("storing index locally")
    new LocalStorage()
  }

  override def read(indexName: IndexName)(implicit ac: AdamContext): Try[DataFrame] = {
    log.info("reading index from storage")
    storage.read(indexName)
  }

  override def drop(indexName: IndexName)(implicit ac: AdamContext): Try[Void] = {
    log.info("dropping index from storage")
    storage.drop(indexName)
  }

  override def write(indexName: IndexName, index: DataFrame)(implicit ac: AdamContext): Try[Void] = {
    log.info("writing index to storage")
    storage.write(indexName, index)
  }

  override def exists(indexname: IndexName): Try[Boolean] = {
    log.info("checking index exists in storage")
    storage.exists(indexname)
  }

  private[engine] trait GenericIndexStorage extends IndexStorage {
    override def read(indexname: IndexName)(implicit ac: AdamContext): Try[DataFrame] = {
      try {
        Success(ac.sqlContext.read.parquet(AdamConfig.indexPath + "/" + indexname + ".parquet"))
      } catch {
        case e: Exception => Failure(e)
      }
    }

    override def write(indexname: IndexName, df: DataFrame)(implicit ac: AdamContext): Try[Void] = {
      try {
        df
          .repartition(AdamConfig.defaultNumberOfPartitions)
          .write.mode(SaveMode.Overwrite).parquet(AdamConfig.indexPath + "/" + indexname + ".parquet")
        Success(null)
      } catch {
        case e: Exception => Failure(e)
      }

    }
  }

  /**
    *
    */
  private class HadoopStorage extends GenericIndexStorage {
    @transient val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", AdamConfig.basePath)

    if (!FileSystem.get(new Path("/").toUri, hadoopConf).exists(new Path(AdamConfig.indexPath))) {
      FileSystem.get(new Path("/").toUri, hadoopConf).mkdirs(new Path(AdamConfig.indexPath))
    }

    override def drop(indexname: IndexName)(implicit ac: AdamContext): Try[Void] = {
      try {
        val path = AdamConfig.indexPath + "/" + indexname + ".parquet"
        val hadoopConf = new Configuration()
        hadoopConf.set("fs.defaultFS", AdamConfig.basePath)
        val hdfs = FileSystem.get(new Path(AdamConfig.indexPath).toUri, hadoopConf)

        val drop = hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
        if (drop) {
          Success(null)
        } else {
          Failure(new Exception("unknown error in dropping"))
        }
      } catch {
        case e: Exception => Failure(e)
      }

    }

    /**
      *
      * @param indexname
      * @return
      */
    override def exists(indexname: IndexName): Try[Boolean] = {
      try {
        val path = AdamConfig.indexPath + "/" + indexname + ".parquet"
        Success(FileSystem.get(new Path(AdamConfig.indexPath).toUri, hadoopConf).exists(new org.apache.hadoop.fs.Path(path)))
      } catch {
        case e: Exception => Failure(e)
      }
    }
  }

  /**
    *
    */
  private class LocalStorage extends GenericIndexStorage {
    val indexFolder = new File(AdamConfig.indexPath)
    log.info("storing indexes to: " + indexFolder.toPath.toAbsolutePath.toString)

    if (!indexFolder.exists()) {
      indexFolder.mkdirs
    }


    override def drop(indexname: IndexName)(implicit ac: AdamContext): Try[Void] = {
      try {
        FileUtils.deleteDirectory(new File(AdamConfig.indexPath + "/" + indexname + ".parquet"))
        Success(null)
      } catch {
        case e: Exception => Failure(e)
      }
    }

    /**
      *
      * @param indexname
      * @return
      */
    override def exists(indexname: IndexName): Try[Boolean] = {
      try {
        Success(new File(AdamConfig.indexPath + "/" + indexname + ".parquet").exists())
      } catch {
        case e: Exception => Failure(e)
      }
    }
  }

}


