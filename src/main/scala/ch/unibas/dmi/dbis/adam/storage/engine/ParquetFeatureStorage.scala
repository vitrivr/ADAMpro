package ch.unibas.dmi.dbis.adam.storage.engine

import java.io.File

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.components.FeatureStorage
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
object ParquetFeatureStorage extends FeatureStorage {
  //TODO: refactor with ParquetIndexStorage
  private def getPath(filename: String): String = AdamConfig.dataPath + "/" + filename

  val storage: GenericFeatureStorage = if (AdamConfig.isBaseOnHadoop) {
    log.debug("storing data on Hadoop")
    new HadoopStorage()
  } else {
    log.debug("storing data locally")
    new LocalStorage()
  }

  override def create(entityname: EntityName, fields: Seq[AttributeDefinition])(implicit ac: AdamContext): Try[Option[String]] = {
    Success(Some(getPath(entityname.toString)))
  }

  override def exists(path: String)(implicit ac: AdamContext): Try[Boolean] = {
    log.debug("checking data exists in " + path)
    storage.exists(path)
  }

  override def count(path: String)(implicit ac: AdamContext): Try[Long] = {
    log.debug("counting data from " + path)
    storage.count(path)
  }

  override def read(path: String)(implicit ac: AdamContext): Try[DataFrame] = {
    log.debug("reading data from " + path)
    storage.read(path)
  }

  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode, path: Option[String] = None, allowRepartitioning: Boolean)(implicit ac: AdamContext): Try[String] = {
    storage.write(entityname, df, mode, path, allowRepartitioning)
  }

  override def drop(path: String)(implicit ac: AdamContext): Try[Void] = {
    log.debug("dropping data from " + path)
    storage.drop(path)
  }


  protected trait GenericFeatureStorage extends FeatureStorage {
    override def read(path: String)(implicit ac: AdamContext): Try[DataFrame] = {
      try {
        if (!exists(path).get) {
          Failure(throw new GeneralAdamException("no file found at " + path))
        }

        var df = ac.sqlContext.read.parquet(path)

        Success(df)
      } catch {
        case e: Exception => Failure(e)
      }
    }

    override def write(entityname: EntityName, df: DataFrame, mode: SaveMode, path: Option[String] = None, allowRepartitioning: Boolean)(implicit ac: AdamContext): Try[String] = {
      try {
        val filepath = path.getOrElse(getPath(entityname.toString))
        log.debug("writing data to " + filepath)

        var data = df

        if (allowRepartitioning) {
          data = data.repartition(AdamConfig.defaultNumberOfPartitions)
        }

        data.write.mode(mode).parquet(filepath)
        Success(filepath)
      } catch {
        case e: Exception => Failure(e)
      }
    }

    override def count(path: String)(implicit ac: AdamContext): Try[Long] = {
      try {
        Success(read(path).get.count())
      } catch {
        case e: Exception => Failure(e)
      }
    }
  }

  /**
    *
    */
  class HadoopStorage extends GenericFeatureStorage {
    @transient val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", AdamConfig.basePath)

    if (!FileSystem.get(new Path("/").toUri, hadoopConf).exists(new Path(AdamConfig.dataPath))) {
      FileSystem.get(new Path("/").toUri, hadoopConf).mkdirs(new Path(AdamConfig.dataPath))
    }

    def drop(path: String)(implicit ac: AdamContext): Try[Void] = {
      try {
        val hadoopConf = new Configuration()
        hadoopConf.set("fs.defaultFS", AdamConfig.basePath)
        val hdfs = FileSystem.get(new Path(AdamConfig.dataPath).toUri, hadoopConf)
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
    override def exists(path: String)(implicit ac: AdamContext): Try[Boolean] = {
      try {
        Success(FileSystem.get(new Path(AdamConfig.dataPath).toUri, hadoopConf).exists(new org.apache.hadoop.fs.Path(path)))
      } catch {
        case e: Exception => Failure(e)
      }
    }
  }

  /**
    *
    */
  class LocalStorage extends GenericFeatureStorage {
    val dataFolder = new File(AdamConfig.dataPath)
    log.debug("storing data to: " + dataFolder.toPath.toAbsolutePath.toString)

    if (!dataFolder.exists()) {
      dataFolder.mkdirs
    }


    def drop(path: String)(implicit ac: AdamContext): Try[Void] = {
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
    override def exists(path: String)(implicit ac: AdamContext): Try[Boolean] = {
      try {
        Success(new File(path).exists())
      } catch {
        case e: Exception => Failure(e)
      }
    }
  }

}
