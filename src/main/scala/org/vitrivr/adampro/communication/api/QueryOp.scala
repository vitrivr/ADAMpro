package org.vitrivr.adampro.communication.api

import org.vitrivr.adampro.data.entity.Entity._
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.data.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.query.ast.generic.{QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.ast.internal.BooleanFilterExpression.BooleanFilterScanExpression
import org.vitrivr.adampro.query.ast.internal._
import org.vitrivr.adampro.query.execution.ProgressiveObservation
import org.vitrivr.adampro.query.query.{FilteringQuery, RankingQuery}
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.query.execution.parallel.{ParallelPathChooser, ParallelQueryHandler, ParallelQueryStatusTracker}

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
  def expression(q: QueryExpression, options: Option[QueryEvaluationOptions] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Try[Option[DataFrame]] = {
    execute("query execution operation") {
      log.trace(QUERY_MARKER, "query operation")
      val prepared = q.rewrite()
      log.trace(QUERY_MARKER, "prepared tree")
      val res = prepared.execute(options)(tracker)
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
  def sequential(entityname: EntityName, nnq: RankingQuery, bq: Option[FilteringQuery], options: Option[QueryEvaluationOptions] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Try[Option[DataFrame]] = {
    execute("sequential query operation") {
      var scan: Option[QueryExpression] = None

      if (bq.isDefined) {
        log.trace("boolean query is defined")
        scan = Some(new BooleanFilterScanExpression(entityname)(bq.get, None)(scan)(ac))
      }

      scan = Some(new SequentialScanExpression(entityname)(nnq, None)(scan)(ac))

      return Success(scan.get.rewrite().execute(options)(tracker))
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
  def index(indexname: IndexName, nnq: RankingQuery, bq: Option[FilteringQuery], options: Option[QueryEvaluationOptions] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Try[Option[DataFrame]] = {
    execute("specified index query operation") {
      val index = Index.load(indexname).get

      var scan: Option[QueryExpression] = None

      if (bq.isDefined) {
        log.trace("boolean query is defined")
        scan = Some(new BooleanFilterScanExpression(index.entityname)(bq.get, None)(scan))
      }

      scan = Some(IndexScanExpression(index)(nnq, None)(scan)(ac))

      Success(scan.get.rewrite().execute(options)(tracker))
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
  def entityIndex(entityname: EntityName, indextypename: IndexTypeName, nnq: RankingQuery, bq: Option[FilteringQuery], options: Option[QueryEvaluationOptions] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Try[Option[DataFrame]] = {
    execute("index query operation") {
      var scan: Option[QueryExpression] = None

      if (bq.isDefined) {
        log.trace("boolean query is defined")
        scan = Some(new BooleanFilterScanExpression(entityname)(bq.get, None)(scan)(ac))
      }

      scan = Some(new IndexScanExpression(entityname, indextypename)(nnq, None)(scan)(ac))

      Success(scan.get.rewrite().execute(options)(tracker))
    }
  }

  /**
    * Performs a parallel query, i.e., all indexes and sequential search are started at the same time and results are returned as soon
    * as they are available. When a precise result is returned, the whole query is stopped.
    *
    * @param entityname  name of entity
    * @param nnq         information for nearest neighbour query
    * @param pathChooser parallel query path chooser
    * @param onNext  operation to perform as soon as one index returns results
    * @param options     options applied when evaluating query
    * @return a tracker for the parallel query
    */
  def parallel[U](entityname: EntityName, nnq: RankingQuery, bq: Option[FilteringQuery], pathChooser: ParallelPathChooser, onNext: Try[ProgressiveObservation] => U, options: Option[QueryEvaluationOptions] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Try[ParallelQueryStatusTracker] = {
    Success(ParallelQueryHandler.parallelQuery(entityname, nnq, bq, pathChooser, onNext, options, None)(tracker))
  }


  /**
    * Performs a timed parallel query, i.e., it performs the query for a maximum of the given time limit and returns then the best possible
    * available results.
    *
    * @param entityname  name of entity
    * @param nnq         information for nearest neighbour query
    * @param pathChooser parallel query path chooser
    * @param timelimit   maximum time to wait
    * @param options     options applied when evaluating query
    * @return the results available together with a confidence score
    */
  def timedParallel(entityname: EntityName, nnq: RankingQuery, bq: Option[FilteringQuery], pathChooser: ParallelPathChooser, timelimit: Duration, options: Option[QueryEvaluationOptions] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Try[ProgressiveObservation] = {
    execute("timed parallel query operation") {
      Success(ParallelQueryHandler.timedParallelQuery(entityname, nnq, bq, pathChooser, timelimit, options, None)(tracker))
    }
  }

  /**
    * Performs a query which uses index compounding for pre-filtering.
    *
    * @param expr    query expression
    * @param options options applied when evaluating query
    * @return
    */
  def compoundQuery(expr: QueryExpression, options: Option[QueryEvaluationOptions] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Try[Option[DataFrame]] = {
    execute("compound query operation") {
      Success(CompoundQueryExpression(expr).execute(options)(tracker))
    }
  }

  /**
    * Performs a boolean query.
    *
    * @param entityname name of entitty
    * @param bq         information for boolean query
    * @param options    options applied when evaluating query
    */
  def booleanQuery(entityname: EntityName, bq: Option[FilteringQuery], options: Option[QueryEvaluationOptions] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Try[Option[DataFrame]] = {
    execute("boolean query operation") {
      var scan: Option[QueryExpression] = None

      if (bq.isDefined) {
        log.trace("boolean query is defined")
        scan = Some(new BooleanFilterScanExpression(entityname)(bq.get, None)(scan)(ac))
      }

      return Success(scan.get.execute(options)(tracker))
    }
  }
}