package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{ProgressiveQueryStatus, ProgressiveQueryStatusTracker, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints._
import ch.unibas.dmi.dbis.adam.query.handler.internal._
import ch.unibas.dmi.dbis.adam.query.progressive.{ProgressivePathChooser, ProgressiveQueryHandler}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
  * adamtwo
  *
  *
  * Ivan Giangreco
  * November 2015
  */
object QueryOp extends Logging {
  /**
    * Executes a query expression.
    *
    * @param q
    * @return
    */
  def apply(q: QueryExpression)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      log.debug("performing query execution operation")
      Success(q.evaluate())
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Performs a standard query, built up by a nearest neighbour query and a boolean query.
    *
    * @param entityname   name of entity
    * @param hint         query hint, for the executor to know which query path to take (e.g., sequential query or index query)
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def apply(entityname: EntityName, hint: Seq[QueryHint], nnq: Option[NearestNeighbourQuery], bq: Option[BooleanQuery], withMetadata: Boolean)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      log.debug("perform standard query operation")
      var res = StandardQueryHolder(entityname)(hint, nnq, bq, None).evaluate()
      Success(res)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Performs a sequential query, i.e., without using any index structure.
    *
    * @param entityname   name of entity
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def sequential(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      log.debug("perform sequential query operation")
      var res = SequentialQueryHolder(entityname)(nnq, bq, None).evaluate()
      Success(res)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Performs an index-based query.
    *
    * @param indexname    name of index
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def index(indexname: IndexName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      log.debug("perform index query operation")

      val index = Index.load(indexname).get
      var res = IndexQueryHolder(index)(nnq, bq, None).evaluate()
      Success(res)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Performs an index-based query.
    *
    * @param entityname    name of entity
    * @param indextypename name of index type
    * @param nnq           information for nearest neighbour query
    * @param bq            information for boolean query
    * @param withMetadata  whether or not to retrieve corresponding metadata
    * @return
    */
  def index(entityname: EntityName, indextypename: IndexTypeName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      log.debug("perform index query operation")
      var res = new IndexQueryHolder(entityname, indextypename)(nnq, bq, None).evaluate()
      Success(res)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Performs a progressive query, i.e., all indexes and sequential search are started at the same time and results are returned as soon
    * as they are available. When a precise result is returned, the whole query is stopped.
    *
    * @param entityname
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param onComplete   operation to perform as soon as one index returns results
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return a tracker for the progressive query
    */
  def progressive[U](entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], paths: ProgressivePathChooser, onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, String, Map[String, String]) => U, withMetadata: Boolean)(implicit ac: AdamContext): Try[ProgressiveQueryStatusTracker] = {
    try {
      log.debug("perform progressive query operation")
      Success(ProgressiveQueryHandler.progressiveQuery(entityname)(nnq, bq, None, paths, onComplete, withMetadata))
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    * Performs a timed progressive query, i.e., it performs the query for a maximum of the given time limit and returns then the best possible
    * available results.
    *
    * @param entityname
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param timelimit    maximum time to wait
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return the results available together with a confidence score
    */
  def timedProgressive(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], paths: ProgressivePathChooser, timelimit: Duration, withMetadata: Boolean)(implicit ac: AdamContext): Try[(DataFrame, Float, String)] = {
    try {
      log.debug("perform timed progressive query operation")
      Success(ProgressiveQueryHandler.timedProgressiveQuery(entityname)(nnq, bq, None, paths, timelimit, withMetadata))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Performs a query which uses index compounding for pre-filtering.
    *
    * @param entityname
    * @param expr
    * @param withMetadata
    * @return
    */
  def compoundQuery(entityname: EntityName, expr: QueryExpression, withMetadata: Boolean)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      log.debug("perform compound query operation")
      var res = CompoundQueryHolder(entityname)(expr).evaluate()
      Success(res)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Performs a boolean query.
    *
    * @param entityname
    * @param bq
    */
  def booleanQuery(entityname: EntityName, bq: Option[BooleanQuery])(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      log.debug("perform boolean query operation")
      Success(BooleanQueryHolder(entityname)(bq).evaluate())
    } catch {
      case e: Exception => Failure(e)
    }
  }
}