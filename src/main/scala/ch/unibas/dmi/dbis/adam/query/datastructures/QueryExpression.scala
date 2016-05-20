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

  /**
    *
    * @param filter
    * @return
    */
  def evaluate(filter: Option[DataFrame] = None)(implicit ac: AdamContext): DataFrame = {
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
  protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame


  /**
    *
    * @param info
    * @return
    */
  private[query] def getRunDetails(info: ListBuffer[RunDetails]) = {
    info += RunDetails(id.getOrElse(""), time, this.getClass.getName, results)
  }
}
