package ch.unibas.dmi.dbis.adam.storage.engine.instances

import java.io.File

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.FileEngine
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
class ParquetEngine(private val subengine: GenericParquetEngine) extends FileEngine with Logging with Serializable {
  def this(localpath: String) {
    this(new ParquetLocalEngine(localpath))
  }

  def this(basepath: String, datapath: String) {
    this(new ParquetHadoopStorage(basepath, datapath))
  }

  def create(filename: String)(implicit ac: AdamContext): Try[Void] = {
    log.debug("parquet create operation")
    Success(null)
  }

  def exists(filename: String)(implicit ac: AdamContext): Try[Boolean] = {
    log.debug("parquet exists operation")
    subengine.exists(filename)
  }

  def read(filename: String)(implicit ac: AdamContext): Try[DataFrame] = {
    log.debug("parquet read operation")
    subengine.read(filename)
  }

  def write(filename: String, df: DataFrame, mode: SaveMode = SaveMode.Append, allowRepartitioning: Boolean = false)(implicit ac: AdamContext): Try[Void] = {
    log.debug("parquet write operation")
    subengine.write(filename, df, mode, allowRepartitioning)
  }

  def drop(filename: String)(implicit ac: AdamContext): Try[Void] = {
    log.debug("parquet drop operation")
    subengine.drop(filename)
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: ParquetEngine => this.subengine.equals(that.subengine)
      case _ => false
    }

  override def hashCode: Int = subengine.hashCode
}

abstract class GenericParquetEngine(filepath: String) extends Logging with Serializable {
  def read(filename: String)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      if (!exists(filename).get) {
        throw new GeneralAdamException("no file found at " + filename)
      }

      Success(ac.sqlContext.read.parquet(filepath + "/" + filename))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  def write(filename: String, df: DataFrame, mode: SaveMode = SaveMode.Append, allowRepartitioning: Boolean)(implicit ac: AdamContext): Try[Void] = {
    try {
      var data = df

      if (allowRepartitioning) {
        data = data.repartition(AdamConfig.defaultNumberOfPartitions)
      }

      data.write.mode(mode).parquet(filepath + "/" + filename)

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  def exists(path: String)(implicit ac: AdamContext): Try[Boolean]

  def drop(path: String)(implicit ac: AdamContext): Try[Void]
}

/**
  *
  * @param basepath will be used as fs.defaultFS in this HadoopStorage
  * @param filepath this path will be used when dropping files or verifying their existence. It is also given to the genericParquetEngine
  */
class ParquetHadoopStorage(private val basepath: String, private val filepath: String) extends GenericParquetEngine(filepath) with Logging with Serializable {
  @transient private val hadoopConf = new Configuration()
  hadoopConf.set("fs.defaultFS", basepath)

  if (!FileSystem.get(new Path("/").toUri, hadoopConf).exists(new Path(filepath))) {
    FileSystem.get(new Path("/").toUri, hadoopConf).mkdirs(new Path(filepath))
  }

  override def drop(filename: String)(implicit ac: AdamContext): Try[Void] = {
    try {
      val hdfs = FileSystem.get(new Path(filepath).toUri, hadoopConf)
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


  override def exists(filename: String)(implicit ac: AdamContext): Try[Boolean] = {
    try {
      val exists = FileSystem.get(hadoopConf).exists(new org.apache.hadoop.fs.Path(filepath + "/" + filename))

      Success(exists)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: ParquetHadoopStorage => this.basepath.equals(that.basepath) && this.filepath.equals(that.filepath)
      case _ => false
    }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + basepath.hashCode
    result = prime * result + filepath.hashCode
    result
  }
}

/**
  *
  */
class ParquetLocalEngine(private val filepath: String) extends GenericParquetEngine(filepath) with Logging with Serializable {
  val dataFolder = new File(filepath)

  if (!dataFolder.exists()) {
    dataFolder.mkdirs
  }


  override def drop(filename: String)(implicit ac: AdamContext): Try[Void] = {
    try {
      FileUtils.deleteDirectory(new File(filepath, filename))
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }


  override def exists(filename: String)(implicit ac: AdamContext): Try[Boolean] = {
    try {
      Success(new File(filepath, filename).exists())
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: ParquetLocalEngine => this.filepath.equals(that.filepath)
      case _ => false
    }

  override def hashCode(): Int = filepath.hashCode
}
