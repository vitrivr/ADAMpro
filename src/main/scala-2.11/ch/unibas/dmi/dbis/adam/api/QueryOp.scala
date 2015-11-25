package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints._
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}

import scala.concurrent.duration.Duration

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object QueryOp {
  def apply(entityname: EntityName, hint: Option[QueryHint], nnq : NearestNeighbourQuery, bq : Option[BooleanQuery], withMetadata : Boolean) : Seq[Result] = {
    QueryHandler.query(entityname, None, nnq, bq, withMetadata)
  }

  def sequential(entityname: EntityName, nnq : NearestNeighbourQuery, bq : Option[BooleanQuery], withMetadata : Boolean) : Seq[Result] = {
    QueryHandler.sequentialQuery(entityname)(nnq, bq, withMetadata)
  }

  def index(indexname: IndexName, nnq : NearestNeighbourQuery, bq : Option[BooleanQuery], withMetadata : Boolean) : Seq[Result] = {
    QueryHandler.indexQuery(indexname)(nnq, bq, withMetadata)
  }

  def progressive(entityname: EntityName, nnq : NearestNeighbourQuery, bq : Option[BooleanQuery], onComplete : (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit, withMetadata : Boolean) : Int = {
    QueryHandler.progressiveQuery(entityname)(nnq, bq, onComplete, withMetadata)
  }

  def progressive(entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], timelimit: Duration, withMetadata: Boolean) : (Seq[Result], Float) = {
    QueryHandler.timedProgressiveQuery(entityname)(nnq, bq, timelimit, withMetadata)
  }
}


