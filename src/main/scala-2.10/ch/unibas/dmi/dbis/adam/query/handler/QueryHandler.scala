package ch.unibas.dmi.dbis.adam.query.handler

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints._
import ch.unibas.dmi.dbis.adam.query.progressive._
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.spark.Logging

import scala.collection.immutable.HashSet
import scala.concurrent.duration.Duration

/**
  * adamtwo
  *
  * Ivan Giangreco
  * November 2015
  */
object QueryHandler extends Logging {
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
  def query(entityname: EntityName, hint: Option[QueryHint], nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): Seq[Result] = {
    val indexes: Map[IndexTypeName, Seq[IndexName]] = CatalogOperator.listIndexesWithType(entityname).groupBy(_._2).mapValues(_.map(_._1))

    var plan = choosePlan(entityname, indexes, hint)

    if (!plan.isDefined) {
      plan = choosePlan(entityname, indexes, Option(QueryHints.FALLBACK_HINTS))
    }

    plan.get(nnq, bq, withMetadata)
  }

  /**
    * Chooses the query plan to use based on the given hint, the available indexes, etc.
    *
    * @param entityname
    * @param indexes
    * @param hint
    * @return
    */
  private def choosePlan(entityname: EntityName, indexes: Map[IndexTypeName, Seq[IndexName]], hint: Option[QueryHint]): Option[(NearestNeighbourQuery, Option[BooleanQuery], Boolean) => Seq[Result]] = {
    if (!hint.isDefined) {
      return None
    }

    hint.get match {
      case iqh: IndexQueryHint => {
        //index scan
        val indexChoice = indexes.get(iqh.structureType)

        if (indexChoice.isDefined) {
          return Option(indexQuery(indexChoice.get.head)) //TODO: use old measurements for choice rather than head
        } else {
          return None
        }
      }
      case SEQUENTIAL_QUERY => return Option(sequentialQuery(entityname)) //sequential
      case cqh: CompoundQueryHint => {
        //compound query hint
        val hints = cqh.hints
        var i = 0

        while (i < hints.length) {
          val plan = choosePlan(entityname, indexes, Option(hints(i)))
          if (plan.isDefined) return plan
        }

        return None
      }
      case _ => None //default
    }
  }


  /**
    * Performs a sequential query, i.e., without using any index structure.
    *
    * @param entityname
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def sequentialQuery(entityname: EntityName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): Seq[Result] = {
    var res = NearestNeighbourQueryHandler.sequential(entityname, nnq, getFilter(entityname, bq))
    if (withMetadata) {
      res = joinWithMetadata(entityname, res)
    }
    res
  }


  /**
    * Performs a index-based query.
    *
    * @param indexname
    * @param nnq          information for nearest neighbour query
    * @param bq           information for boolean query
    * @param withMetadata whether or not to retrieve corresponding metadata
    * @return
    */
  def indexQuery(indexname: IndexName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): Seq[Result] = {
    val entityname = Index.load(indexname).entityname
    var res = NearestNeighbourQueryHandler.indexQuery(indexname, nnq, getFilter(entityname, bq))
    if (withMetadata) {
      res = joinWithMetadata(indexname, res)
    }
    res
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
  def progressiveQuery(entityname: EntityName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], onComplete: (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit, withMetadata: Boolean): ProgressiveQueryStatusTracker = {
    val onCompleteFunction = if (withMetadata) {
      (pqs: ProgressiveQueryStatus.Value, res: Seq[Result], conf: Float, info: Map[String, String]) => onComplete(pqs, joinWithMetadata(entityname, res), conf, info)
    } else {
      onComplete
    }

    NearestNeighbourQueryHandler.progressiveQuery(entityname, nnq, getFilter(entityname, bq), onCompleteFunction)
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
  def timedProgressiveQuery(entityname: EntityName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], timelimit: Duration, withMetadata: Boolean): (Seq[Result], Float) = {
    var (res, confidence) = NearestNeighbourQueryHandler.timedProgressiveQuery(entityname, nnq, getFilter(entityname, bq), timelimit)
    if (withMetadata) {
      res = joinWithMetadata(entityname, res)
    }
    (res, confidence)
  }


  /**
    * Creates a filter that is applied on the nearest neighbour search based on the Boolean query.
    *
    * @param entityname
    * @param bq
    * @return
    */
  private def getFilter(entityname: EntityName, bq: Option[BooleanQuery]): Option[HashSet[TupleID]] = {
    if (bq.isDefined) {
      val mdRes = BooleanQueryHandler.metadataQuery(entityname, bq.get)
      Option(HashSet(mdRes.map(r => r.getLong(0)): _*))
    } else {
      None
    }
  }


  /**
    * Joins the results from the nearest neighbour query with the metadata.
    *
    * @param entityname
    * @param res
    * @return
    */
  private def joinWithMetadata(entityname: EntityName, res: Seq[Result]): Seq[Result] = {
    val metadata = BooleanQueryHandler.metadataQuery(entityname, HashSet(res.map(_.tid): _*)).map(r => r.getLong(0) -> r).toMap

    res.map(r => {
      r.metadata = metadata.get(r.tid)
      r
    })
  }
}
