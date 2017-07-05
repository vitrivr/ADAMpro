package org.vitrivr.adampro.storage.engine

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{StructField, StructType}
import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.data.entity.AttributeDefinition
import org.vitrivr.adampro.process.SharedComponentContext

import scala.util.{Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class ParquetIndexEngine()(@transient override implicit val ac: SharedComponentContext) extends ParquetEngine()(ac) {
  override val name = "parquetindex"

  override def supports = Seq()

  override def specializes = Seq()

  /**
    *
    * @param props
    */
  def this(props: Map[String, String])(implicit ac: SharedComponentContext) {
    this()(ac)
    if (props.get("hadoop").getOrElse("false").toBoolean) {
      subengine = new ParquetHadoopStorage(AdamConfig.cleanPath(props.get("basepath").get), props.get("datapath").get)
    } else {
      subengine = new ParquetLocalEngine(AdamConfig.cleanPath(props.get("path").get))
    }
  }

  /**
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes attributes of the entity (w.r.t. handler)
    * @param params     creation parameters
    * @return options to store
    */
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    log.debug("parquet create operation")
    Success(Map())
  }
}
