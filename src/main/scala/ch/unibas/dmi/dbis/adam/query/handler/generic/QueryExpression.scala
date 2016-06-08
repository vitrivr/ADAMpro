package ch.unibas.dmi.dbis.adam.query.handler.generic

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.information.InformationLevels._
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
  private val DEFAULT_WEIGHT = 0

  private var prepared = false
  private var run = false

  private var results: Option[DataFrame] = None
  val info = ExpressionDetails(None, None, id, None)
  protected var children: Seq[QueryExpression] = Seq()

  /**
    * The filter can be set to speed up queries
    */
  var filter: Option[DataFrame] = None

  /**
    * Makes adjustments to the tree
    */
  def prepareTree(): QueryExpression = {
    if (!prepared) {
      prepared = true
      children = children.map(_.prepareTree())
    } else {
      log.warn("expression was already prepared, still preparing children")
      children = children.map(_.prepareTree())
    }

    this
  }

  /**
    * Evaluates the query expression. Note that you should run prepareTree() before evaluating a query expression.
    * @return
    */
  def evaluate()(implicit ac: AdamContext): Option[DataFrame] = {
    if (!prepared) {
      log.warn("expression should be prepared before running")
    }

    val t1 = System.currentTimeMillis
    results = run(filter)
    run = true
    val t2 = System.currentTimeMillis

    //TODO: possibly log time at production time and use a command to update based on the stored logs the weights of each index
    info.time = Duration(t2 - t1, TimeUnit.MILLISECONDS)

    results
  }


  /**
    *
    * @param filter filter to apply to data
    * @return
    */
  protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): Option[DataFrame]


  /**
    * Returns information on the query expression (possibly on the tree)
    *
    * @param levels degree of detail in collecting information
    * @return
    */
  def information(levels: Seq[InformationLevel]): ListBuffer[ExpressionDetails] = {
    var withResults: Boolean = true
    var maxDepth: Int = Int.MaxValue

    levels.foreach {
      case FULL_TREE => maxDepth = Int.MaxValue
      case INTERMEDIATE_RESULTS => withResults = true
      case LAST_STEP_ONLY => maxDepth = 0
    }

    information(0, maxDepth, withResults)
  }


  /**
    * Returns information on the query expression (possibly on the tree).
    *
    * @return
    */
  def information(): ExpressionDetails = {
    information(0, 0, withResults = true).head
  }

  /**
    * Returns information on the query expression (possibly on the tree).
    *
    * @param currentDepth current depth in query expression tree
    * @param maxDepth     maximum depth to scan to in tree
    * @param withResults  denotes whether the results should be retrieved as well (computationally expensive!)
    * @param lb           list buffer to write information to
    * @return
    */
  private def information(currentDepth: Int = 0, maxDepth: Int = Int.MaxValue, withResults: Boolean, lb: ListBuffer[ExpressionDetails] = new ListBuffer[ExpressionDetails]()): ListBuffer[ExpressionDetails] = {
    if (!run) {
      log.warn("expression should be run before trying to receive information")
    }

    if (withResults || currentDepth == 0) {
      info.results = results
    }

    lb += info

    if (maxDepth == 0) {
      return lb
    }

    children.foreach {
      child => child.information(currentDepth + 1, maxDepth, withResults, lb)
    }

    lb
  }

  /**
    * Returns string of expression and connection children.
    *
    * @param indentation number of spaces to indent to
    * @return
    */
  def mkString(indentation: Int = 0): String = {
    val sb = new StringBuffer()
    sb.append(info.mkString(indentation))

    children.foreach {
      child =>
        sb.append(child.mkString(indentation + 4))
    }

    sb.toString
  }
}

case class ExpressionDetails(source: Option[String], scantype: Option[String], id: Option[String], confidence: Option[Float]) {
  var time: Duration = Duration.Zero
  var results: Option[DataFrame] = None

  def mkString(indentation: Int = 0): String = {
    val sb = new StringBuffer()
    sb.append(" " * indentation)
    sb.append(scantype.getOrElse("<unknown scan type>"))

    if (source.isDefined) {
      sb.append(" (")
      sb.append(source.get)
      sb.append(")")
    }

    sb.append("\n")
    sb.append(" " * indentation)
    sb.append(id.getOrElse("<unknown id>"))

    sb.append("\n")

    sb.toString
  }
}
