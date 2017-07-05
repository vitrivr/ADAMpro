package org.vitrivr.adampro.storage.engine

import java.io.File

import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.entity.AttributeDefinition
import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.vitrivr.adampro.query.query.Predicate
import org.vitrivr.adampro.utils.Logging
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.vitrivr.adampro.process.SharedComponentContext

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
class ParquetEngine()(@transient override implicit val ac: SharedComponentContext) extends Engine()(ac) with Logging with Serializable {
  override val name = "parquet"

  override def supports = Seq(AttributeTypes.AUTOTYPE, AttributeTypes.INTTYPE, AttributeTypes.LONGTYPE, AttributeTypes.FLOATTYPE, AttributeTypes.DOUBLETYPE, AttributeTypes.STRINGTYPE, AttributeTypes.TEXTTYPE, AttributeTypes.BOOLEANTYPE, AttributeTypes.GEOGRAPHYTYPE, AttributeTypes.GEOMETRYTYPE, AttributeTypes.VECTORTYPE, AttributeTypes.SPARSEVECTORTYPE)

  override def specializes = Seq(AttributeTypes.VECTORTYPE, AttributeTypes.SPARSEVECTORTYPE)

  var subengine: GenericParquetEngine = _

  override val repartitionable = true

  /**
    *
    * @param props
    */
  def this(props: Map[String, String])(implicit ac: SharedComponentContext) {
    this()(ac)
    if (props.get("hadoop").getOrElse("false").toBoolean) {
      subengine = new ParquetHadoopStorage(ac.config.cleanPath(props.get("basepath").get), props.get("datapath").get)
    } else {
      subengine = new ParquetLocalEngine(ac.config.cleanPath(props.get("path").get))
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
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    log.debug("parquet create operation")

    val schema = StructType(attributes.map(attribute => StructField(attribute.name.toString, attribute.attributeType.datatype)))
    write(storename, ac.spark.createDataFrame(ac.sc.emptyRDD[Row], schema), attributes, SaveMode.ErrorIfExists, params)
  }

  /**
    * Check if entity exists.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def exists(storename: String)(implicit ac: SharedComponentContext): Try[Boolean] = {
    log.debug("parquet exists operation")
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
  override def read(storename: String, attributes: Seq[AttributeDefinition], predicates: Seq[Predicate], params: Map[String, String])(implicit ac: SharedComponentContext): Try[DataFrame] = {
    log.debug("parquet read operation")
    subengine.read(storename)
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
  override def write(storename: String, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode = SaveMode.Append, params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    log.debug("parquet write operation")
    val allowRepartitioning = params.getOrElse("allowRepartitioning", "false").toBoolean

    import org.apache.spark.sql.functions.col

    var data = df

    if (allowRepartitioning) {
      val partitioningKey = params.get("partitioningKey")

      if (partitioningKey.isDefined) {
        data = data.repartition(ac.config.defaultNumberOfPartitions, col(partitioningKey.get))
      } else {
        data = data.repartition(ac.config.defaultNumberOfPartitions)
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
  def drop(storename: String)(implicit ac: SharedComponentContext): Try[Void] = {
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

/**
  *
  */
trait GenericParquetEngine extends Logging with Serializable {
  def read(filename: String)(implicit ac: SharedComponentContext): Try[DataFrame]

  def write(filename: String, df: DataFrame, mode: SaveMode = SaveMode.Append)(implicit ac: SharedComponentContext): Try[Void]

  def exists(path: String)(implicit ac: SharedComponentContext): Try[Boolean]

  def drop(path: String)(implicit ac: SharedComponentContext): Try[Void]
}


/**
  *
  */
class ParquetHadoopStorage(private val basepath: String, private val datapath: String) extends GenericParquetEngine with Logging with Serializable {
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
  def read(filename: String)(implicit ac: SharedComponentContext): Try[DataFrame] = {
    try {
      if (!exists(filename).get) {
        throw new GeneralAdamException("no file found at " + fullHadoopPath)
      }

      val df = Success(ac.sqlContext.read.parquet(fullHadoopPath + filename))

      df
    } catch {
      case e : org.apache.spark.sql.AnalysisException => Failure(new GeneralAdamException("no data available for file at " + filename))
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
  def write(filename: String, df: DataFrame, mode: SaveMode = SaveMode.Append)(implicit ac: SharedComponentContext): Try[Void] = {
    try {
      df.write.mode(mode).parquet(fullHadoopPath + filename)
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
  override def drop(filename: String)(implicit ac: SharedComponentContext): Try[Void] = {
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
  override def exists(filename: String)(implicit ac: SharedComponentContext): Try[Boolean] = {
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
class ParquetLocalEngine(private val path: String) extends GenericParquetEngine with Logging with Serializable {
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
  def read(filename: String)(implicit ac: SharedComponentContext): Try[DataFrame] = {
    try {
      if (!exists(filename).get) {
        throw new GeneralAdamException("no file found at " + path + filename)
      }

      val df = Success(ac.sqlContext.read.parquet(sparkPath + filename))

      df
    } catch {
      case e : org.apache.spark.sql.AnalysisException => Failure(new GeneralAdamException("no data available for file at " + filename))
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
  def write(filename: String, df: DataFrame, mode: SaveMode = SaveMode.Append)(implicit ac: SharedComponentContext): Try[Void] = {
    try {
      df.write.mode(mode).parquet(sparkPath + filename)
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
  override def drop(filename: String)(implicit ac: SharedComponentContext): Try[Void] = {
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
  override def exists(filename: String)(implicit ac: SharedComponentContext): Try[Boolean] = {
    try {
      Success(new File(path, filename).exists())
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: ParquetLocalEngine => this.path.equals(that.path)
      case _ => false
    }

  override def hashCode(): Int = path.hashCode
}
