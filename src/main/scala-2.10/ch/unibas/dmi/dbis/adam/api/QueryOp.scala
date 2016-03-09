package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints._
import ch.unibas.dmi.dbis.adam.query.progressive.{ProgressiveQueryStatusTracker, ProgressiveQueryStatus}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}

import scala.concurrent.duration.Duration

/**
  * adamtwo
  *
  * Query operation. Performs different types of queries
  *
  * Ivan Giangreco
  * November 2015
  */
object QueryOp {
  /**
    * Performs a standard query, built up by a nearest neighbour query and a boolean query.
    *
    * @param entityname
    * @param hint query hint, for the executor to know which query path to take (e.g., sequential query or index query)
    * @param nnq information for nearest neighbour query
    * @param bq information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def apply(entityname: EntityName, hint: Option[QueryHint], nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): Seq[Result] =
    QueryHandler.query(entityname, hint, nnq, bq, withMetadata)

  /**
    * Performs a sequential query, i.e., without using any index structure.
    *
    * @param entityname
    * @param nnq information for nearest neighbour query
    * @param bq information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def sequential(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): Seq[Result] =
    QueryHandler.sequentialQuery(entityname)(nnq, bq, withMetadata)


  /**
    * Performs an index-based query.
    *
    * @param indexname
    * @param nnq information for nearest neighbour query
    * @param bq information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def index(indexname: IndexName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): Seq[Result] =
    QueryHandler.indexQuery(indexname)(nnq, bq, withMetadata)


  /**
    * Performs a progressive query, i.e., all indexes and sequential search are started at the same time and results are returned as soon
    * as they are available. When a precise result is returned, the whole query is stopped.
    *
    * @param entityname
    * @param nnq information for nearest neighbour query
    * @param bq information for boolean query
    * @param onComplete operation to perform as soon as one index returns results
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return a tracker for the progressive query
    */
  def progressive(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], onComplete: (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit, withMetadata: Boolean): ProgressiveQueryStatusTracker =
    QueryHandler.progressiveQuery(entityname)(nnq, bq, onComplete, withMetadata)


  /**
    * Performs a timed progressive query, i.e., it performs the query for a maximum of the given time limit and returns then the best possible
    * available results.
    *
    * @param entityname
    * @param nnq information for nearest neighbour query
    * @param bq information for boolean query
    * @param timelimit maximum time to wait
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return the results available together with a confidence score
    */
  def timedProgressive(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], timelimit: Duration, withMetadata: Boolean): (Seq[Result], Float) =
    QueryHandler.timedProgressiveQuery(entityname)(nnq, bq, timelimit, withMetadata)

}


