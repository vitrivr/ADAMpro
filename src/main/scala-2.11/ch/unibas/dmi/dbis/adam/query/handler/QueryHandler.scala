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
   *
   * @param entityname
   * @param hint
   * @param nnq
   * @param bq
   * @param withMetadata
   * @return
   */
  def query(entityname: EntityName, hint: Option[QueryHint], nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): Seq[Result] = {
    val indexes: Map[IndexTypeName, Seq[IndexName]] = CatalogOperator.getIndexesAndType(entityname).groupBy(_._2).mapValues(_.map(_._1))

    var plan = choosePlan(entityname, indexes, hint)

    if (!plan.isDefined) {
      plan = choosePlan(entityname, indexes, Option(QueryHints.FALLBACK_HINTS))
    }

    plan.get(nnq, bq, withMetadata)
  }

  /**
   *
   * @param entityname
   * @param indexes
   * @param hint
   * @return
   */
  private def choosePlan(entityname: EntityName, indexes: Map[IndexTypeName, Seq[IndexName]], hint: Option[QueryHint]): Option[(NearestNeighbourQuery, Option[BooleanQuery], Boolean) => Seq[Result]] = {
    if(!hint.isDefined){
      return None
    }

    hint.get match {
      case iqh: IndexQueryHint => {  //index scan
        val indexChoice = indexes.get(iqh.structureType)

        if (indexChoice.isDefined) {
          return Option(indexQuery(indexChoice.get.head)) //TODO: use old measurements for choice rather than head
        } else {
          return None
        }
      }
      case SEQUENTIAL_QUERY => return Option(sequentialQuery(entityname)) //sequential
      case cqh: CompoundQueryHint => { //compound query hint
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
   *
   * @param entityname
   * @param nnq
   * @param bq
   * @param withMetadata
   * @return
   */
  private def sequentialQuery(entityname: EntityName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): Seq[Result] = {
    var res = NearestNeighbourQueryHandler.sequentialQuery(entityname, nnq, getFilter(entityname, bq))
    if (withMetadata) {
      res = joinWithMetadata(entityname, res)
    }
    res
  }


  /**
   *
   * @param indexname
   * @param nnq
   * @param bq
   * @param withMetadata
   * @return
   */
  private def indexQuery(indexname: IndexName)(nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], withMetadata: Boolean): Seq[Result] = {
    val entityname = Index.retrieveIndex(indexname).entityname
    var res = NearestNeighbourQueryHandler.indexQuery(indexname, nnq, getFilter(entityname, bq))
    if (withMetadata) {
      res = joinWithMetadata(indexname, res)
    }
    res
  }

  /**
   *
   * @param entityname
   * @param nnq
   * @param bq
   * @param onComplete
   * @param withMetadata
   * @return
   */
  def progressiveQuery(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], onComplete: (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit, withMetadata: Boolean): Int = {
    val onCompleteFunction = if (withMetadata) {
      (pqs: ProgressiveQueryStatus.Value, res: Seq[Result], conf: Float, info: Map[String, String]) => onComplete(pqs, joinWithMetadata(entityname, res), conf, info)
    } else {
      onComplete
    }

    NearestNeighbourQueryHandler.progressiveQuery(entityname, nnq, getFilter(entityname, bq), onCompleteFunction)
  }


  /**
   *
   * @param entityname
   * @param nnq
   * @param bq
   * @param timelimit
   * @param withMetadata
   * @return
   */
  def progressiveQuery(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], timelimit: Duration, withMetadata: Boolean): (Seq[Result], Float) = {
    var (res, confidence) = NearestNeighbourQueryHandler.progressiveQuery(entityname, nnq, getFilter(entityname, bq), timelimit)
    if (withMetadata) {
      res = joinWithMetadata(entityname, res)
    }
    (res, confidence)
  }


  /**
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
