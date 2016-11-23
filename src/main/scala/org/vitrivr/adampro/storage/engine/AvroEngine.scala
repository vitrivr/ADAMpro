package org.vitrivr.adampro.storage.engine

import java.io.File

import com.databricks.spark.avro._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.datatypes.FieldTypes
import org.vitrivr.adampro.datatypes.feature.{FeatureVectorWrapperUDT, FeatureVectorWrapper}
import org.vitrivr.adampro.entity.AttributeDefinition
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.query.Predicate
import org.vitrivr.adampro.utils.Logging

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
class AvroEngine extends Engine with Logging with Serializable {
  override val name = "avro"

  override def supports = Seq(FieldTypes.AUTOTYPE, FieldTypes.SERIALTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.TEXTTYPE, FieldTypes.BOOLEANTYPE, FieldTypes.FEATURETYPE)

  override def specializes = Seq(FieldTypes.FEATURETYPE)

  var subengine: GenericAvroEngine = _

  /**
    *
    * @param props
    */
  def this(props: Map[String, String]) {
    this()
    if (props.get("hadoop").getOrElse("false").toBoolean) {
      subengine = new AvroHadoopStorage(AdamConfig.cleanPath(props.get("basepath").get), props.get("datapath").get)
    } else {
      subengine = new AvroLocalEngine(AdamConfig.cleanPath(props.get("path").get))
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
    log.debug("avro create operation")
    Success(Map())
  }

  /**
    * Check if entity exists.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def exists(storename: String)(implicit ac: AdamContext): Try[Boolean] = {
    log.debug("avro exists operation")
    subengine.exists(storename)
  }

  /**
    * Read entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes the attributes to read
    * @param predicates filtering predicates (only applied if possible)
    * @param params     reading parameters
    * @return
    */
  override def read(storename: String, attributes: Seq[AttributeDefinition], predicates: Seq[Predicate], params: Map[String, String])(implicit ac: AdamContext): Try[DataFrame] = {
    log.debug("avro read operation")
    import org.apache.spark.sql.functions.udf
    val castToFeature = udf((c: Seq[Float]) => {
      new FeatureVectorWrapper(c)
    })

    try {
      val res = subengine.read(storename)

      if (res.isSuccess) {
        var data = res.get

        res.get.schema.fields.filter(_.dataType.isInstanceOf[ArrayType]).foreach { field =>
          data = data.withColumn(field.name, castToFeature(col(field.name)))
        }

        Success(data)
      } else {
        res
      }
    } catch {
      case e: Exception =>
        log.error("fatal error when reading from avro", e)
        Failure(e)
    }
  }

  /**
    * Write entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param df         data
    * @param attributes attributes to store
    * @param mode       save mode (append, overwrite, ...)
    * @param params     writing parameters
    * @return new options to store
    */
  override def write(storename: String, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode = SaveMode.Append, params: Map[String, String])(implicit ac: AdamContext): Try[Map[String, String]] = {
    log.debug("avro write operation")
    val allowRepartitioning = params.getOrElse("allowRepartitioning", "false").toBoolean

    import org.apache.spark.sql.functions.{col, udf}

    var data = df

    if (allowRepartitioning) {
      val partitioningKey = params.get("partitioningKey")

      if (partitioningKey.isDefined) {
        data = data.repartition(AdamConfig.defaultNumberOfPartitions, col(partitioningKey.get))
      } else {
        data = data.repartition(AdamConfig.defaultNumberOfPartitions)
      }
    }

    val arrayUDF = udf((c: FeatureVectorWrapper) => {
      try {
        if (c != null) {
          c.toSeq.toArray[Float]
        } else {
          Array[Float]()
        }
      } catch {
        case e: Exception =>
          log.error("error when converting to avro", e)
          Array[Float]()
      }
    })

    df.schema.fields.filter(_.dataType.isInstanceOf[FeatureVectorWrapperUDT]).foreach { field =>
      data = data.withColumn(field.name, arrayUDF(col(field.name)))
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
    log.debug("avro drop operation")
    subengine.drop(storename)
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: AvroEngine => this.subengine.equals(that.subengine)
      case _ => false
    }

  override def hashCode: Int = subengine.hashCode
}

/**
  *
  */
trait GenericAvroEngine extends Logging with Serializable {
  def read(filename: String)(implicit ac: AdamContext): Try[DataFrame]

  def write(filename: String, df: DataFrame, mode: SaveMode = SaveMode.Append)(implicit ac: AdamContext): Try[Void]

  def exists(path: String)(implicit ac: AdamContext): Try[Boolean]

  def drop(path: String)(implicit ac: AdamContext): Try[Void]
}


/**
  *
  */
class AvroHadoopStorage(private val basepath: String, private val datapath: String) extends GenericAvroEngine with Logging with Serializable {
  @transient private val hadoopConf = new Configuration()
  hadoopConf.set("fs.defaultFS", basepath)

  if (!FileSystem.get(new Path("/").toUri, hadoopConf).exists(new Path(datapath))) {
    FileSystem.get(new Path("/").toUri, hadoopConf).mkdirs(new Path(datapath))
  }

  //we assume:
  // basepath (hdfs://...) ends on "/"
  // datapath (/.../.../) is given absolutely and starts with "/" and ends with "/"
  private val fullHadoopPath = basepath + datapath.substring(1)


  /**
    *
    * @param filename
    * @return
    */
  def read(filename: String)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      if (!exists(filename).get) {
        throw new GeneralAdamException("no file found at " + fullHadoopPath)
      }

      Success(ac.sqlContext.read.avro(fullHadoopPath + filename))
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
      df.write.mode(mode).avro(fullHadoopPath + filename)
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
  override def drop(filename: String)(implicit ac: AdamContext): Try[Void] = {
    try {
      val hdfs = FileSystem.get(new Path(datapath).toUri, hadoopConf)
      val drop = hdfs.delete(new Path(datapath, filename), true)

      if (drop) {
        Success(null)
      } else {
        Failure(new Exception("unknown error in dropping file " + filename))
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
      val hdfs = FileSystem.get(new Path(datapath).toUri, hadoopConf)
      val exists = hdfs.exists(new Path(datapath, filename))
      Success(exists)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: AvroHadoopStorage => this.basepath.equals(that.basepath) && this.datapath.equals(that.datapath)
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
class AvroLocalEngine(private val path: String) extends GenericAvroEngine with Logging with Serializable {
  val sparkPath = "file://" + path
  val datafolder = new File(path)

  if (!datafolder.exists()) {
    datafolder.mkdirs
  }


  /**
    *
    * @param filename
    * @return
    */
  def read(filename: String)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      if (!exists(filename).get) {
        throw new GeneralAdamException("no file found at " + path + filename)
      }

      Success(ac.sqlContext.read.avro(sparkPath + filename))
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
      df.write.mode(mode).avro(sparkPath + filename)
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
  override def drop(filename: String)(implicit ac: AdamContext): Try[Void] = {
    try {
      FileUtils.deleteDirectory(new File(path, filename))
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
      Success(new File(path, filename).exists())
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: AvroLocalEngine => this.path.equals(that.path)
      case _ => false
    }

  override def hashCode(): Int = path.hashCode
}
