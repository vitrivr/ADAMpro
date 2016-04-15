package ch.unibas.dmi.dbis.adam.main

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import scala.annotation.implicitNotFound

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
@implicitNotFound("Cannot find an implicit AdamContext, either import SparkStartup.Implicits._ or use a custom one")
trait AdamContext {
  def sc : SparkContext
  def sqlContext : HiveContext
}
