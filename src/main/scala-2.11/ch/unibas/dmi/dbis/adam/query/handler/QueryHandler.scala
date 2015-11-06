package ch.unibas.dmi.dbis.adam.query.handler

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.progressive._
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.apache.spark.Logging

import scala.collection.immutable.HashSet
import scala.concurrent.duration.Duration

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object QueryHandler extends Logging { //TODO: refactor! better design

  /**
   *
   * @param entityname
   * @param nnq
   * @param bq
   * @param withMetadata
   * @return
   */
  def sequentialQuery(entityname: EntityName, nnq : NearestNeighbourQuery, bq : Option[BooleanQuery], withMetadata : Boolean): Seq[Result] = {
    val res = if(bq.isDefined){
      val mdRes = BooleanQueryHandler.metadataQuery(entityname, bq.get)
      val filter = HashSet(mdRes.map(r => r.getLong(0)) : _*)
      NearestNeighbourQueryHandler.sequentialQuery(entityname, nnq, Option(filter))
    } else {
      NearestNeighbourQueryHandler.sequentialQuery(entityname, nnq, None)
    }


    if(withMetadata){
      val metadata = BooleanQueryHandler.metadataQuery(entityname, HashSet(res.map(_.tid) : _* )).map(r => r.getLong(0) -> r).toMap
      return res.map(r => {r.metadata = metadata.get(r.tid); r})
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
  def indexQuery(indexname : IndexName, nnq : NearestNeighbourQuery, bq : Option[BooleanQuery], withMetadata : Boolean): Seq[Result] = {
    val entityname = Index.retrieveIndex(indexname).entityname

    val res = if(bq.isDefined){
      val mdRes = BooleanQueryHandler.metadataQuery(entityname, bq.get)
      val filter = HashSet(mdRes.map(r => r.getLong(0)) : _*)
      NearestNeighbourQueryHandler.indexQuery(indexname, nnq, Option(filter))
    } else {
      NearestNeighbourQueryHandler.indexQuery(indexname, nnq, None)
    }

    if(withMetadata){
      val metadata = BooleanQueryHandler.metadataQuery(entityname, HashSet(res.map(_.tid) : _* )).map(r => r.getLong(0) -> r).toMap
      return res.map(r => {r.metadata = metadata.get(r.tid); r})
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
  def progressiveQuery(entityname: EntityName, nnq : NearestNeighbourQuery, bq : Option[BooleanQuery], onComplete: (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit, withMetadata : Boolean): Int = {
    val onCompleteFunction = if(withMetadata){
      (pqs : ProgressiveQueryStatus.Value, res : Seq[Result], conf : Float, info : Map[String, String]) => {
        val metadata = BooleanQueryHandler.metadataQuery(entityname, HashSet(res.map(_.tid) : _* )).map(r => r.getLong(0) -> r).toMap
        val resWithMetadata = res.map(r => {r.metadata = metadata.get(r.tid); r})
        onComplete(pqs, resWithMetadata, conf, info)
      }
    } else {
      onComplete
    }

    if(bq.isDefined){
      val mdRes = BooleanQueryHandler.metadataQuery(entityname, bq.get)
      val filter = HashSet(mdRes.map(r => r.getLong(0)) : _*)
      NearestNeighbourQueryHandler.progressiveQuery(entityname, nnq, Option(filter), onCompleteFunction)
    } else {
      NearestNeighbourQueryHandler.progressiveQuery(entityname, nnq, None, onCompleteFunction)
    }
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
  def timedProgressiveQuery(entityname: EntityName, nnq : NearestNeighbourQuery, bq : Option[BooleanQuery], timelimit : Duration, withMetadata : Boolean): (Seq[Result], Float) = {
    val resAndConfidence = if(bq.isDefined){
      val mdRes = BooleanQueryHandler.metadataQuery(entityname, bq.get)
      val filter = HashSet(mdRes.map(r => r.getLong(0)) : _*)
      NearestNeighbourQueryHandler.timedProgressiveQuery(entityname, nnq, Option(filter), timelimit)
    } else {
      NearestNeighbourQueryHandler.timedProgressiveQuery(entityname, nnq, None, timelimit)
    }

    val res = resAndConfidence._1

    if(withMetadata){
      val metadata = BooleanQueryHandler.metadataQuery(entityname, HashSet(res.map(_.tid) : _* )).map(r => r.getLong(0) -> r).toMap
      return (res.map(r => {r.metadata = metadata.get(r.tid); r}), resAndConfidence._2)
    }

    (res, resAndConfidence._2)
  }
}
