package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.api.CompoundQueryOp.Expression
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints._
import ch.unibas.dmi.dbis.adam.query.progressive.{ProgressiveQueryStatus, ProgressiveQueryStatusTracker}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

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
  val log = Logger.getLogger(getClass.getName)

  /**
    * Performs a standard query, built up by a nearest neighbour query and a boolean query.
    *
    * @param entityname
    * @param hint         query hint, for the executor to know which query path to take (e.g., sequential query or index query)
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def apply(entityname: EntityName, hint: Option[QueryHint], nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): DataFrame = {
    log.debug("perform standard query operation")
    QueryHandler.query(entityname, hint, nnq, bq, withMetadata)
  }

  case class StandardQueryHolder(entityname: EntityName, hint: Option[QueryHint], nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean) extends Expression {
    override def eval() = apply(entityname, hint, nnq, bq, withMetadata)
  }
  def apply(q: StandardQueryHolder): DataFrame = q.eval()

  /**
    * Performs a sequential query, i.e., without using any index structure.
    *
    * @param entityname
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def sequential(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): DataFrame = {
    log.debug("perform sequential query operation")
    QueryHandler.sequentialQuery(entityname)(nnq, bq, withMetadata)
  }

  case class SequentialQueryHolder(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean) extends Expression {
    override def eval() = sequential(entityname, nnq, bq, withMetadata)
  }
  def sequential(q: SequentialQueryHolder): DataFrame = q.eval()


  /**
    * Performs an index-based query.
    *
    * @param indexname
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def index(indexname: IndexName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): DataFrame = {
    log.debug("perform index query operation")
    QueryHandler.indexQuery(indexname)(nnq, bq, withMetadata)
  }

  case class SpecifiedIndexQueryHolder(indexname: IndexName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean) extends Expression {
    override def eval() = index(indexname, nnq, bq, withMetadata)
  }
  def index(q: SpecifiedIndexQueryHolder): DataFrame = q.eval()

  /**
    * Performs an index-based query.
    *
    * @param entityname
    * @param indextypename
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def index(entityname: EntityName, indextypename: IndexTypeName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): DataFrame = {
    log.debug("perform index query operation")
    QueryHandler.indexQuery(entityname, indextypename)(nnq, bq, withMetadata)
  }

  case class IndexQueryHolder(entityname: EntityName, indextypename: IndexTypeName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean)  extends Expression {
    override def eval() = index(entityname, indextypename, nnq, bq, withMetadata)
  }
  def index(q: IndexQueryHolder): DataFrame = q.eval()

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
  def progressive[U](entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, String, Map[String, String]) => U, withMetadata: Boolean): ProgressiveQueryStatusTracker = {
    log.debug("perform progressive query operation")
    QueryHandler.progressiveQuery(entityname)(nnq, bq, onComplete, withMetadata)
  }

  case class ProgressiveQueryHolder[U](entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, String, Map[String, String]) => U, withMetadata: Boolean)

  def progressive(q: ProgressiveQueryHolder[_]) : ProgressiveQueryStatusTracker = progressive(q.entityname, q.nnq, q.bq, q.onComplete, q.withMetadata)


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
  def timedProgressive(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], timelimit: Duration, withMetadata: Boolean): (DataFrame, Float, String) = {
    log.debug("perform timed progressive query operation")
    QueryHandler.timedProgressiveQuery(entityname)(nnq, bq, timelimit, withMetadata)
  }

  case class TimedProgressiveQueryHolder(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], timelimit: Duration, withMetadata: Boolean)

  def timedProgressive(q: TimedProgressiveQueryHolder): (DataFrame, Float, String) = timedProgressive(q.entityname, q.nnq, q.bq, q.timelimit, q.withMetadata)
}