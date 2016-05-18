package ch.unibas.dmi.dbis.adam.storage.engine

import java.io.File

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.EntityNotExistingException
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
  val storage: GenericFeatureStorage = if (AdamConfig.isBaseOnHadoop) {
    log.debug("storing data on Hadoop")
    new HadoopStorage()
  } else {
    log.debug("storing data locally")
    new LocalStorage()
  }

  override def exists(entityname: EntityName): Try[Boolean] = {
    storage.exists(entityname)
  }

  override def count(entityname: EntityName)(implicit ac: AdamContext): Try[Long] = storage.count(entityname)

  override def drop(entityname: EntityName)(implicit ac: AdamContext): Try[Void] = storage.drop(entityname)

  override def write(entityname: EntityName, pk: String, df: DataFrame, mode: SaveMode)(implicit ac: AdamContext): Try[Void] = storage.write(entityname, pk, df, mode)

  override def read(entityname: EntityName)(implicit ac: AdamContext): Try[DataFrame] = storage.read(entityname)


  protected trait GenericFeatureStorage extends FeatureStorage {
    override def read(entityname: EntityName)(implicit ac: AdamContext): Try[DataFrame] = {
      try {
        if (!exists(entityname).get) {
          Failure(throw EntityNotExistingException())
        }

        var df = ac.sqlContext.read.parquet(AdamConfig.dataPath + "/" + entityname + ".parquet").coalesce(AdamConfig.defaultNumberOfPartitions)

        Success(df)
      } catch {
        case e: Exception => Failure(e)
      }
    }

    override def write(entityname: EntityName, pk: String, df: DataFrame, mode: SaveMode)(implicit ac: AdamContext): Try[Void] = {
      try {
        df
          .repartition(AdamConfig.defaultNumberOfPartitions)
          .write.mode(mode).parquet(AdamConfig.dataPath + "/" + entityname + ".parquet")
        Success(null)
      } catch {
        case e: Exception => Failure(e)
      }
    }


    override def count(entityname: EntityName)(implicit ac: AdamContext): Try[Long] = {
      try{
        Success(read(entityname).get.count())
      } catch {
        case e : Exception => Failure(e)
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

    def drop(entityname: EntityName)(implicit ac: AdamContext): Try[Void] = {
      try {
        val path = AdamConfig.dataPath + "/" + entityname + ".parquet"
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
      * @param entityname
      * @return
      */
    override def exists(entityname: EntityName): Try[Boolean] = {
      try {
        val path = AdamConfig.dataPath + "/" + entityname + ".parquet"
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

    if (!dataFolder.exists()) {
      dataFolder.mkdirs
    }


    def drop(entityname: EntityName)(implicit ac: AdamContext): Try[Void] = {
      try {
        FileUtils.deleteDirectory(new File(AdamConfig.dataPath + "/" + entityname + ".parquet"))
        Success(null)
      } catch {
        case e: Exception => Failure(e)
      }
    }

    /**
      *
      * @param entityname
      * @return
      */
    override def exists(entityname: EntityName): Try[Boolean] = {
      try {
        Success(new File(AdamConfig.dataPath + "/" + entityname + ".parquet").exists())
      } catch {
        case e: Exception => Failure(e)
      }
    }
  }

}
