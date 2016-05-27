package ch.unibas.dmi.dbis.adam.query.datastructures

import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
case class RunDetails(id : String, time : Duration, source : String, results : Option[DataFrame])

