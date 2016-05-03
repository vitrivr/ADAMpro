package ch.unibas.dmi.dbis.adam.query.datastructures

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
abstract class QueryExpression(id: Option[String]) {
  private var time: Duration = null
  private var results : DataFrame = null

  /**
    *
    * @param filter
    * @return
    */
  def evaluate(filter: Option[DataFrame] = None): DataFrame = {
    val t1 = System.currentTimeMillis
    results = run(filter)
    val t2 = System.currentTimeMillis

    time = Duration(t2 - t1, TimeUnit.MILLISECONDS)

    results
  }

  /**
    *
    * @param filter
    * @return
    */
  protected def run(filter: Option[DataFrame]): DataFrame


  /**
    *
    * @param info
    * @return
    */
  private[query] def getRunDetails(info : ListBuffer[RunDetails]) = {
    info += RunDetails(id.getOrElse(""), time, this.getClass.getName, results)
  }
}
