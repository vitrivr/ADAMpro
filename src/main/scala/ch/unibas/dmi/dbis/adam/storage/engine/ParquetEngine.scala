package ch.unibas.dmi.dbis.adam.storage.engine

import java.io.File

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
class ParquetEngine extends Engine with Logging with Serializable {
  override val name = "parquet"

  override def supports = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.TEXTTYPE, FieldTypes.BOOLEANTYPE, FieldTypes.FEATURETYPE)

  override def specializes = Seq(FieldTypes.FEATURETYPE)

  var subengine: GenericParquetEngine = _

  /**
    *
    * @param basepath
    * @param datapath
    * @param hadoop
    */
  def this(basepath: String, datapath: String, hadoop: Boolean) {
    this()
    if (hadoop) {
      subengine = new ParquetHadoopStorage(basepath, datapath)
    } else {
      subengine = new ParquetLocalEngine(basepath, datapath)
    }

    subengine = new ParquetHadoopStorage(basepath, datapath)
  }

  /**
    *
    * @param props
    */
  def this(props: Map[String, String]) {
    this()
    if (props.get("hadoop").getOrElse("false").toBoolean) {
      subengine = new ParquetHadoopStorage(AdamConfig.cleanPath(props.get("basepath").get), props.get("datapath").get)
    } else {
      subengine = new ParquetLocalEngine(AdamConfig.cleanPath(props.get("basepath").get), props.get("datapath").get)
    }
  }

  /**
    * Create the entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes attributes of the entity (w.r.t. handler)
    * @param params     creation parameters
    * @return options to store
    */
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: AdamContext): Try[Map[String, String]] = {
    log.debug("parquet create operation")
    Success(Map())
  }

  /**
    * Check if entity exists.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def exists(storename: String)(implicit ac: AdamContext): Try[Boolean] = {
    log.debug("parquet exists operation")
    subengine.exists(storename)
  }

  /**
    * Read entity.
    *
    * @param storename adapted entityname to store feature to
    * @param params    reading parameters
    * @return
    */
  override def read(storename: String, params: Map[String, String])(implicit ac: AdamContext): Try[DataFrame] = {
    log.debug("parquet read operation")
    subengine.read(storename)
  }

  /**
    * Write entity.
    *
    * @param storename adapted entityname to store feature to
    * @param df        data
    * @param mode      save mode (append, overwrite, ...)
    * @param params    writing parameters
    * @return new options to store
    */
  override def write(storename: String, df: DataFrame, mode: SaveMode = SaveMode.Append, params: Map[String, String])(implicit ac: AdamContext): Try[Map[String, String]] = {
    log.debug("parquet write operation")
    val allowRepartitioning = params.getOrElse("allowRepartitioning", "false").toBoolean

    import org.apache.spark.sql.functions._

    var data = df

    if (allowRepartitioning) {
      val partitioningKey = params.get("partitioningKey")

      if (partitioningKey.isDefined) {
        data = data.repartition(AdamConfig.defaultNumberOfPartitions, col(partitioningKey.get))
      } else {
        data = data.repartition(AdamConfig.defaultNumberOfPartitions)
      }
    }

    val res = subengine.write(storename, data, mode)


    if (res.isSuccess) {
      Success(Map())
    } else {
      Failure(res.failed.get)
    }

  }

  /**
    * Drop the entity.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  def drop(storename: String)(implicit ac: AdamContext): Try[Void] = {
    log.debug("parquet drop operation")
    subengine.drop(storename)
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: ParquetEngine => this.subengine.equals(that.subengine)
      case _ => false
    }

  override def hashCode: Int = subengine.hashCode
}

abstract class GenericParquetEngine(protected val basepath: String, protected val datapath : String) extends Logging with Serializable {
  /**
    *
    * @param filename
    * @return
    */
  def read(filename: String)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      if (!exists(filename).get) {
        throw new GeneralAdamException("no file found at " + filename)
      }

      Success(ac.sqlContext.read.parquet(basepath + "/" + datapath + "/" + filename))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param filename
    * @param df
    * @param mode
    * @return
    */
  def write(filename: String, df: DataFrame, mode: SaveMode = SaveMode.Append)(implicit ac: AdamContext): Try[Void] = {
    try {
      df.write.mode(mode).parquet(basepath + "/" + datapath + "/" + filename)
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
  def exists(path: String)(implicit ac: AdamContext): Try[Boolean]

  /**
    *
    * @param path
    * @return
    */
  def drop(path: String)(implicit ac: AdamContext): Try[Void]
}

/**
  *
  */
class ParquetHadoopStorage(override protected val basepath: String, override protected val datapath: String) extends GenericParquetEngine(basepath, datapath) with Logging with Serializable {
  @transient private val hadoopConf = new Configuration()
  hadoopConf.set("fs.defaultFS", basepath)

  if (!FileSystem.get(new Path("/").toUri, hadoopConf).exists(new Path(datapath))) {
    FileSystem.get(new Path("/").toUri, hadoopConf).mkdirs(new Path(datapath))
  }

  /**
    *
    * @param filename
    * @return
    */
  override def drop(filename: String)(implicit ac: AdamContext): Try[Void] = {
    try {
      val hdfs = FileSystem.get(new Path(basepath + "/" + datapath).toUri, hadoopConf)
      val drop = hdfs.delete(new org.apache.hadoop.fs.Path(filename), true)

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
    * @param filename
    * @return
    */
  override def exists(filename: String)(implicit ac: AdamContext): Try[Boolean] = {
    try {
      val exists = FileSystem.get(hadoopConf).exists(new org.apache.hadoop.fs.Path(datapath + "/" + filename))

      Success(exists)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: ParquetHadoopStorage => this.basepath.equals(that.basepath) && this.datapath.equals(that.datapath)
      case _ => false
    }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + basepath.hashCode
    result = prime * result + datapath.hashCode
    result
  }
}

/**
  *
  */
class ParquetLocalEngine(override protected val basepath : String, override protected val datapath : String) extends GenericParquetEngine(basepath, datapath) with Logging with Serializable {
  val datafolder = new File(basepath, datapath)

  if (!datafolder.exists()) {
    datafolder.mkdirs
  }

  /**
    *
    * @param filename
    * @return
    */
  override def drop(filename: String)(implicit ac: AdamContext): Try[Void] = {
    try {
      FileUtils.deleteDirectory(new File(new File(basepath, datapath), filename))
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param filename
    * @return
    */
  override def exists(filename: String)(implicit ac: AdamContext): Try[Boolean] = {
    try {
      Success(new File(new File(basepath, datapath), filename).exists())
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: ParquetLocalEngine => this.basepath.equals(that.basepath) && this.datapath.equals(that.datapath)
      case _ => false
    }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + basepath.hashCode
    result = prime * result + datapath.hashCode
    result
  }
}
