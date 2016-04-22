package ch.unibas.dmi.dbis.adam.storage.engine

import java.io.File

import ch.unibas.dmi.dbis.adam.config.{AdamConfig, FieldNames}
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.components.FeatureStorage
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
object ParquetFeatureStorage extends FeatureStorage {
  val log = Logger.getLogger(getClass.getName)

  val storage: GenericFeatureStorage = if (AdamConfig.isBaseOnHadoop) {
    log.debug("storing data on Hadoop")
    new HadoopStorage()
  } else {
    log.debug("storing data locally")
    new LocalStorage()
  }

  override def create(entityname: EntityName)(implicit ac: AdamContext): Boolean = {
    true
  }

  override def exists(entityname: EntityName): Boolean = storage.exists(entityname)

  override def count(entityname: EntityName)(implicit ac: AdamContext): Int = storage.count(entityname)

  override def drop(entityname: EntityName)(implicit ac: AdamContext): Boolean = storage.drop(entityname)

  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode)(implicit ac: AdamContext): Boolean = storage.write(entityname, df, mode)

  override def read(entityname: EntityName, filter: Option[Set[TupleID]])(implicit ac: AdamContext): DataFrame = storage.read(entityname, filter)


  protected trait GenericFeatureStorage extends FeatureStorage {
    override def read(entityname: EntityName, filter: Option[Set[TupleID]])(implicit ac: AdamContext): DataFrame = {
      val df = ac.sqlContext.read.parquet(AdamConfig.dataPath + "/" + entityname + ".parquet")

      if (filter.isDefined) {
        df.filter(df(FieldNames.idColumnName) isin (filter.get.toSeq: _*))
      } else {
        df
      }
    }

    override def write(entityname: EntityName, df: DataFrame, mode: SaveMode)(implicit ac: AdamContext): Boolean = {
      df
        .repartition(AdamConfig.defaultNumberOfPartitions, df(FieldNames.idColumnName))
        .write.mode(mode).parquet(AdamConfig.dataPath + "/" +  entityname + ".parquet")
      true
    }


    override def count(entityname: EntityName)(implicit ac: AdamContext): Int = {
      read(entityname).count().toInt
    }
  }

  /**
    *
    */
  class HadoopStorage extends GenericFeatureStorage {
    val hadoopConf = new Configuration()

    hadoopConf.set("fs.defaultFS", AdamConfig.basePath)

    if (!FileSystem.get(new Path("/").toUri, hadoopConf).exists(new Path(AdamConfig.dataPath))) {
      FileSystem.get(new Path("/").toUri, hadoopConf).mkdirs(new Path(AdamConfig.dataPath))
    }

    def drop(entityname: EntityName)(implicit ac: AdamContext): Boolean = {
      val path = AdamConfig.indexPath + "/" + entityname + ".parquet"
      val hadoopConf = new Configuration()
      hadoopConf.set("fs.defaultFS", AdamConfig.basePath)
      val hdfs = FileSystem.get(new Path(AdamConfig.dataPath).toUri, hadoopConf)

      hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
      true

    }

    /**
      *
      * @param entityname
      * @return
      */
    override def exists(entityname: EntityName): Boolean = {
      val path = AdamConfig.dataPath + "/" + entityname + ".parquet"
      FileSystem.get(new Path(AdamConfig.dataPath).toUri, hadoopConf).exists(new org.apache.hadoop.fs.Path(path))
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


    def drop(entityname: EntityName)(implicit ac: AdamContext): Boolean = {
      FileUtils.deleteDirectory(new File(AdamConfig.dataPath + "/" + entityname + ".parquet"))
      true
    }

    /**
      *
      * @param entityname
      * @return
      */
    override def exists(entityname: EntityName): Boolean = {
      new File(AdamConfig.dataPath + "/" + entityname + ".parquet").exists()
    }
  }

}
