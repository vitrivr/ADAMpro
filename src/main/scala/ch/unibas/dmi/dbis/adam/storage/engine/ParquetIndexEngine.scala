package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.config.AdamConfig

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class ParquetIndexEngine extends ParquetEngine {
  override val name = "index"

  override def supports = Seq()

  override def specializes = Seq()

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
}
