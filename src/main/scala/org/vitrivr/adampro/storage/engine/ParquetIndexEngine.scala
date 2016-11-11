package org.vitrivr.adampro.storage.engine

import org.vitrivr.adampro.config.AdamConfig

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class ParquetIndexEngine extends ParquetEngine {
  override val name = "parquetindex"

  override def supports = Seq()

  override def specializes = Seq()

  /**
    *
    * @param props
    */
  def this(props: Map[String, String]) {
    this()
    if (props.get("hadoop").getOrElse("false").toBoolean) {
      subengine = new ParquetHadoopStorage(AdamConfig.cleanPath(props.get("basepath").get), props.get("datapath").get)
    } else {
      subengine = new ParquetLocalEngine(AdamConfig.cleanPath(props.get("path").get))
    }
  }
}
