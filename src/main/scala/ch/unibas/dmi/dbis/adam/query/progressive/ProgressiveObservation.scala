package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.query.datastructures.ProgressiveQueryStatus
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class ProgressiveObservation(status: ProgressiveQueryStatus.Value, results: Option[DataFrame], confidence: Float, source: String, info: Map[String, String], t1 : Long, t2 : Long)
