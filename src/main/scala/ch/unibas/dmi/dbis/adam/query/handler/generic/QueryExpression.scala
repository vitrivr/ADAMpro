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
  private var prepared = false
  private var run = false
  private val lock = new Object()

  private var results : Option[DataFrame] = None
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
    *
    * @return
    */
  def evaluate()(implicit ac: AdamContext): Option[DataFrame] = {
    if (!prepared) {
      log.warn("expression should be prepared before running")
    }

    val t1 = System.currentTimeMillis
    lock.synchronized {
      results = run(filter)
      run = true
    }
    val t2 = System.currentTimeMillis


    info.time = Duration(t2 - t1, TimeUnit.MILLISECONDS)

    results
  }

  /**
    *
    * @return
    */
  protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): Option[DataFrame]



  /**
    *
    * @param il degree of detail in collecting information
    * @return
    */
  def information(il : InformationLevel): ListBuffer[ExpressionDetails] = {
    il match {
      case FULL_TREE_NO_INTERMEDIATE_RESULTS => information(withResults = false)
      case FULL_TREE_INTERMEDIATE_RESULTS => information()
      case LAST_STEP_ONLY => information(0)
      case _ => information()
    }
  }

  /**
    *
    * @param level how many levels of depth to consider for collecting information
    * @param lb
    * @return
    */
  private def information(level: Int = Int.MaxValue, withResults : Boolean = true, lb: ListBuffer[ExpressionDetails] = new ListBuffer[ExpressionDetails]()): ListBuffer[ExpressionDetails] = {
    if (!run) {
      log.warn("expression should be run before trying to receive information")
    }

    if(withResults) {
      info.results = results
    }

    lb += info

    if (level == 0) {
      return lb
    }

    children.foreach {
      child => child.information(level - 1, withResults, lb)
    }

    lb
  }

  /**
    *
    * @param indentation
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
