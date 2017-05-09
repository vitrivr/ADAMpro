package org.vitrivr.adampro.query.handler.generic

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.query.information.InformationLevels._
import org.vitrivr.adampro.query.progressive.{ProgressiveObservation, ProgressiveQueryStatus}
import org.vitrivr.adampro.utils.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
abstract class QueryExpression(id: Option[String]) extends Serializable with Logging {
  private val DEFAULT_WEIGHT = 0

  private[handler] var prepared = false
  private var run = false

  private var results: Option[DataFrame] = None
  val info = ExpressionDetails(None, None, id, None, Map())
  protected var _children: Seq[QueryExpression] = Seq()

  /**
    *
    * @return
    */
  def children = _children

  /**
    * The filter can be set to speed up queries
    */
  var filter: Option[DataFrame] = None

  /**
    * Makes adjustments to the tree. Note that you should always perform prepareTree before running evaluate; furthermore,
    * you should perform evaluate() on the "prepared expression" (i.e. on what this method returns) and not on the expression generally
    *
    * @param silent log warning if already prepared
    * @return
    */
  def prepareTree(silent: Boolean = false): QueryExpression = {
    if (!prepared) {
      prepared = true
      _children = _children.map(_.prepareTree())
    } else {
      if (!silent) {
        log.warn("expression was already prepared, still preparing children")
      }
      _children = _children.map(_.prepareTree())
    }

    this
  }

  /**
    * Evaluates the query expression. Note that you should run prepareTree() before evaluating a query expression.
    *
    * @return
    */
  def evaluate(options: Option[QueryEvaluationOptions] = None)(tracker: OperationTracker)(implicit ac: AdamContext): Option[DataFrame] = {
    try {
      if (!prepared) {
        log.warn("expression should be prepared before running")
      }

      val t1 = System.currentTimeMillis
      log.trace(QUERY_MARKER, "before evaluating query")
      results = run(options, filter)(tracker)
      log.trace(QUERY_MARKER, "evaluated query")
      run = true
      val t2 = System.currentTimeMillis

      val time = t2 - t1

      if (ac.config.logQueryExecutionTime && info.source.isDefined) {
        SparkStartup.logOperator.addQuery(this)
      }

      info.time = Duration(time, TimeUnit.MILLISECONDS)


      if (results.isDefined) {
        tracker.addObservation(this, Success(ProgressiveObservation(ProgressiveQueryStatus.RUNNING, results, info.confidence.getOrElse(0.toFloat), info.source.getOrElse(""), info.toMap(), t1, t2)))
      }
    } catch {
      case e: Exception =>
        tracker.addObservation(this, Failure(e))
        throw e
    }

    results
  }


  /**
    *
    * @param filter filter to apply to data
    * @return
    */
  protected def run(options: Option[QueryEvaluationOptions], filter: Option[DataFrame])(tracker: OperationTracker)(implicit ac: AdamContext): Option[DataFrame]


  /**
    * Returns information on the query expression (possibly on the tree)
    *
    * @param levels degree of detail in collecting information
    * @return
    */
  def information(levels: Seq[InformationLevel])(implicit ac: AdamContext): ListBuffer[ExpressionDetails] = {
    var withResults: Boolean = true
    var maxDepth: Int = Int.MaxValue

    levels.foreach {
      case FULL_TREE => maxDepth = Int.MaxValue
      case INTERMEDIATE_RESULTS => withResults = true
      case LAST_STEP_ONLY => maxDepth = 0
      case _ => {}
    }

    information(0, maxDepth, levels, withResults = withResults)
  }


  /**
    * Returns information on the query expression (possibly on the tree).
    *
    * @return
    */
  def information()(implicit ac: AdamContext): ExpressionDetails = {
    information(0, 0, Seq(), withResults = true).head
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
  private def information(currentDepth: Int = 0, maxDepth: Int = Int.MaxValue, levels: Seq[InformationLevel], withResults: Boolean, lb: ListBuffer[ExpressionDetails] = new ListBuffer[ExpressionDetails]())(implicit ac: AdamContext): ListBuffer[ExpressionDetails] = {
    if (!run) {
      log.warn("expression should be run before trying to receive information")
    }

    if (withResults || currentDepth == 0) {
      var _results = results

      if (_results.isDefined && levels.contains(PARTITION_PROVENANCE)) {
        val rdd = results.get.rdd.mapPartitionsWithIndex((idx, iter) => iter.map(r => Row(r.toSeq ++ Seq(idx): _*)), preservesPartitioning = true)
        _results = Some(ac.sqlContext.createDataFrame(rdd, results.get.schema.add(AttributeNames.partitionColumnName, IntegerType)))
      }

      info.results = _results
    }

    lb += info

    if (maxDepth == 0) {
      return lb
    }

    _children.foreach {
      child => child.information(currentDepth + 1, maxDepth, levels, withResults, lb)
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

    _children.foreach {
      child =>
        sb.append(child.mkString(indentation + 4))
    }

    sb.toString
  }
}

case class ExpressionDetails(source: Option[String], scantype: Option[String], id: Option[String], confidence: Option[Float], info: Map[String, String] = Map()) {
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

  def toMap(): Map[String, String] = {
    val _info = mutable.Map[String, String]()

    if (source.isDefined) {
      _info += "source" -> source.get
    }

    if(scantype.isDefined){
      _info += "scantype" -> scantype.get
    }

    if(id.isDefined){
      _info += "confidence" -> id.get
    }

    if(confidence.isDefined){
      _info += "confidence" -> confidence.get.toString
    }

    (_info.toMap ++ info)
  }
}
