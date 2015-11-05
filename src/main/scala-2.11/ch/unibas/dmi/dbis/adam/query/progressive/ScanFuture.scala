package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.query.{QueryHandler, Result}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
abstract class ScanFuture(tracker : ProgressiveQueryStatusTracker){
  val future : Future[_]

  val confidence : Float
}


class IndexScanFuture(indexname : IndexName, query : NearestNeighbourQuery, onComplete: (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit, val tracker : ProgressiveQueryStatusTracker) extends ScanFuture(tracker) {
  tracker.register(this)

  val info =  Map[String,String]("type" -> ("index: " + indexname), "index" -> indexname, "qid" -> query.queryID.get)

  val future = Future {QueryHandler.indexQuery(indexname, query, None)}
  future.onSuccess({
    case res =>
      tracker.synchronized {
        if(tracker.status == ProgressiveQueryStatus.RUNNING){
         onComplete(tracker.status, res, confidence, info)
        }
       tracker.notifyCompletion(this, res)
      }
  })

  lazy val confidence: Float = Index.retrieveIndexConfidence(indexname)
}

//TODO: use query object
class SequentialScanFuture(entityname : EntityName, query : NearestNeighbourQuery, onComplete: (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit, val tracker : ProgressiveQueryStatusTracker) extends ScanFuture(tracker) {
  tracker.register(this)

  val info =  Map[String,String]("type" -> "sequential", "relation" -> entityname, "qid" -> query.queryID.get)

  val future = Future {QueryHandler.sequentialQuery(entityname, query, None)}
  future.onSuccess({
    case res =>
      tracker.synchronized {
        if (tracker.status == ProgressiveQueryStatus.RUNNING) {
          onComplete(tracker.status, res, confidence, info)
        }
        tracker.notifyCompletion(this, res)
      }
  })

  val confidence: Float = 1.toFloat
}