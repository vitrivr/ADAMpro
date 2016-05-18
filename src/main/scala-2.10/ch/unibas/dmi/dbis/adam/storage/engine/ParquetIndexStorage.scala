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
  //TODO: refactor with ParquetFeatureStorage
  private def getPath(filename: String): String = AdamConfig.indexPath + "/" + filename

  val storage: GenericIndexStorage = if (AdamConfig.isBaseOnHadoop) {
    log.debug("storing index on Hadoop")
    new HadoopStorage()
  } else {
    log.debug("storing index locally")
    new LocalStorage()
  }

  override def read(path: String)(implicit ac: AdamContext): Try[DataFrame] = {
    log.debug("reading index from storage")
    storage.read(path)
  }

  override def drop(path: String)(implicit ac: AdamContext): Try[Void] = {
    log.debug("dropping index from storage")
    storage.drop(path)
  }

  override def write(indexName: IndexName, index: DataFrame, path: Option[String] = None, allowRepartitioning: Boolean)(implicit ac: AdamContext): Try[String] = {
    log.debug("writing index to storage")
    storage.write(indexName, index, path, allowRepartitioning)
  }

  override def exists(path: String): Try[Boolean] = {
    log.debug("checking index exists in storage")
    storage.exists(path)
  }

  private[engine] trait GenericIndexStorage extends IndexStorage {
    override def read(path: String)(implicit ac: AdamContext): Try[DataFrame] = {
      try {
        Success(ac.sqlContext.read.parquet(path))
      } catch {
        case e: Exception => Failure(e)
      }
    }

    override def write(indexname: IndexName, df: DataFrame, path: Option[String] = None, allowRepartitioning: Boolean = false)(implicit ac: AdamContext): Try[String] = {
      try {
        val filepath = path.getOrElse(getPath(indexname.toString))
        var data = df

        if (allowRepartitioning) {
          data = data.repartition(AdamConfig.defaultNumberOfPartitions)
        }

        data.write.mode(SaveMode.Overwrite).parquet(filepath)
        Success(filepath)
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

    override def drop(path: String)(implicit ac: AdamContext): Try[Void] = {
      try {
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
      * @param path
      * @return
      */
    override def exists(path: String): Try[Boolean] = {
      try {
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
    log.debug("storing indexes to: " + indexFolder.toPath.toAbsolutePath.toString)

    if (!indexFolder.exists()) {
      indexFolder.mkdirs
    }

    override def drop(path: String)(implicit ac: AdamContext): Try[Void] = {
      try {
        FileUtils.deleteDirectory(new File(path))
        Success(null)
      } catch {
        case e: Exception => Failure(e)
      }
    }

    /**
      *
      * @param path
      * @return
      */
    override def exists(path: String): Try[Boolean] = {
      try {
        Success(new File(path).exists())
      } catch {
        case e: Exception => Failure(e)
      }
    }
  }

}


