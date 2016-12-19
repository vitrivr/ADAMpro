package org.vitrivr.adampro.api

import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.handler.internal.BooleanFilterExpression.BooleanFilterScanExpression
import org.vitrivr.adampro.query.handler.internal._
import org.vitrivr.adampro.query.progressive.{ProgressiveQueryStatusTracker, ProgressiveObservation, ProgressivePathChooser, ProgressiveQueryHandler}
import org.vitrivr.adampro.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

/**
  * adamtwo
  *
  *
  * Ivan Giangreco
  * November 2015
  */
object QueryOp extends GenericOp {
  /**
    * Executes a query expression.
    *
    * @param q       query expression
    * @param options options applied when evaluating query
    * @return
    */
  def apply(q: QueryExpression, options: Option[QueryEvaluationOptions] = None)(implicit ac: AdamContext): Try[Option[DataFrame]] = {
    execute("query execution operation") {
      log.trace(QUERY_MARKER, "query operation")
      val prepared = q.prepareTree()
      log.trace(QUERY_MARKER, "prepared tree")
      val res = prepared.evaluate(options)
      log.trace(QUERY_MARKER, "evaluated query")

      Success(res)
    }
  }


  /**
    * Performs a sequential query, i.e., without using any index structure.
    *
    * @param entityname name of entity
    * @param nnq        information for nearest neighbour query
    * @param bq         information for boolean query
    * @param options    options applied when evaluating query
    * @return
    */
  def sequential(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], options: Option[QueryEvaluationOptions] = None)(implicit ac: AdamContext): Try[Option[DataFrame]] = {
    execute("sequential query operation") {
      var scan: Option[QueryExpression] = None

      if (bq.isDefined) {
        log.trace("boolean query is defined")
        scan = Some(new BooleanFilterScanExpression(entityname)(bq.get)(scan))
      }

      scan = Some(new SequentialScanExpression(entityname)(nnq)(scan))

      return Success(scan.get.prepareTree().evaluate(options))
    }
  }

  /**
    * Performs an index-based query.
    *
    * @param indexname name of index
    * @param nnq       information for nearest neighbour query
    * @param options   options applied when evaluating query
    * @return
    */
  def index(indexname: IndexName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], options: Option[QueryEvaluationOptions] = None)(implicit ac: AdamContext): Try[Option[DataFrame]] = {
    execute("specified index query operation") {
      val index = Index.load(indexname).get

      var scan: Option[QueryExpression] = None

      if (bq.isDefined) {
        log.trace("boolean query is defined")
        scan = Some(new BooleanFilterScanExpression(index.entityname)(bq.get)(scan))
      }

      scan = Some(IndexScanExpression(index)(nnq)(scan))

      Success(scan.get.prepareTree().evaluate(options))
    }
  }

  /**
    * Performs an index-based query.
    *
    * @param entityname    name of entity
    * @param indextypename name of index type
    * @param nnq           information for nearest neighbour query
    * @param options       options applied when evaluating query
    * @return
    */
  def entityIndex(entityname: EntityName, indextypename: IndexTypeName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], options: Option[QueryEvaluationOptions] = None)(implicit ac: AdamContext): Try[Option[DataFrame]] = {
    execute("index query operation") {
      var scan: Option[QueryExpression] = None

      if (bq.isDefined) {
        log.trace("boolean query is defined")
        scan = Some(new BooleanFilterScanExpression(entityname)(bq.get)(scan))
      }

      scan = Some(new IndexScanExpression(entityname, indextypename)(nnq)(scan))

      Success(scan.get.prepareTree().evaluate(options))
    }
  }

  /**
    * Performs a progressive query, i.e., all indexes and sequential search are started at the same time and results are returned as soon
    * as they are available. When a precise result is returned, the whole query is stopped.
    *
    * @param entityname  name of entity
    * @param nnq         information for nearest neighbour query
    * @param pathChooser progressive query path chooser
    * @param onComplete  operation to perform as soon as one index returns results
    * @param options     options applied when evaluating query
    * @return a tracker for the progressive query
    */
  def progressive[U](entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], pathChooser: ProgressivePathChooser, onComplete: Try[ProgressiveObservation] => U, options: Option[QueryEvaluationOptions] = None)(implicit ac: AdamContext): Try[ProgressiveQueryStatusTracker] = {
    Success(ProgressiveQueryHandler.progressiveQuery(entityname, nnq, bq, pathChooser, onComplete, options))
  }


  /**
    * Performs a timed progressive query, i.e., it performs the query for a maximum of the given time limit and returns then the best possible
    * available results.
    *
    * @param entityname  name of entity
    * @param nnq         information for nearest neighbour query
    * @param pathChooser progressive query path chooser
    * @param timelimit   maximum time to wait
    * @param options     options applied when evaluating query
    * @return the results available together with a confidence score
    */
  def timedProgressive(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], pathChooser: ProgressivePathChooser, timelimit: Duration, options: Option[QueryEvaluationOptions] = None)(implicit ac: AdamContext): Try[ProgressiveObservation] = {
    execute("timed progressive query operation") {
      Success(ProgressiveQueryHandler.timedProgressiveQuery(entityname, nnq, bq, pathChooser, timelimit, options))
    }
  }

  /**
    * Performs a query which uses index compounding for pre-filtering.
    *
    * @param expr    query expression
    * @param options options applied when evaluating query
    * @return
    */
  def compoundQuery(expr: QueryExpression, options: Option[QueryEvaluationOptions] = None)(implicit ac: AdamContext): Try[Option[DataFrame]] = {
    execute("compound query operation") {
      Success(CompoundQueryExpression(expr).evaluate(options))
    }
  }

  /**
    * Performs a boolean query.
    *
    * @param entityname name of entitty
    * @param bq         information for boolean query
    * @param options    options applied when evaluating query
    */
  def booleanQuery(entityname: EntityName, bq: Option[BooleanQuery], options: Option[QueryEvaluationOptions] = None)(implicit ac: AdamContext): Try[Option[DataFrame]] = {
    execute("boolean query operation") {
      var scan: Option[QueryExpression] = None

      if (bq.isDefined) {
        log.trace("boolean query is defined")
        scan = Some(new BooleanFilterScanExpression(entityname)(bq.get)(scan))
      }

      return Success(scan.get.evaluate(options))
    }
  }
}