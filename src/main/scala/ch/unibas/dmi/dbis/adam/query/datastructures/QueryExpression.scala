package ch.unibas.dmi.dbis.adam.query.datastructures

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
abstract class QueryExpression(id: Option[String]) extends Serializable with Logging {
  private var time: Duration = null
  private var results: DataFrame = null
  private var run = false

  /**
    *
    * @param input
    * @return
    */
  def evaluate(input: Option[DataFrame] = None)(implicit ac: AdamContext): DataFrame = {
    val t1 = System.currentTimeMillis
    results = run(input)
    run = true
    val t2 = System.currentTimeMillis

    time = Duration(t2 - t1, TimeUnit.MILLISECONDS)

    results
  }

  /**
    *
    * @param filter
    * @return
    */
  protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame


  /**
    *
    * @param info
    * @return
    */
  def getRunDetails(info: ListBuffer[RunDetails]) : ListBuffer[RunDetails] = {
    if (!run) {
      log.warn("please run compound query before collecting run information")
    } else {
      info += RunDetails(id.getOrElse(""), time, this.getClass.getName, results)
    }

    info
  }
}
