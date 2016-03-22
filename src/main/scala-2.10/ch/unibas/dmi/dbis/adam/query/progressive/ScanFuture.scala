package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.query.handler.NearestNeighbourQueryHandler
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
abstract class ScanFuture(tracker : ProgressiveQueryStatusTracker){
  val name : String
  val future : Future[_]
  val confidence : Float
}

/**
  * Scan future for indexes.
  *
  * @param indexname
  * @param query
  * @param onComplete
  * @param tracker
  */
class IndexScanFuture(indexname : IndexName, query : NearestNeighbourQuery, onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, String, Map[String, String]) => Unit, val tracker : ProgressiveQueryStatusTracker) extends ScanFuture(tracker) {
  tracker.register(this)

  val name = Index.indextype(indexname).name
  val info =  Map[String,String]("type" -> ("index: " + indexname), "index" -> indexname, "qid" -> query.queryID.get)

  val future = Future {NearestNeighbourQueryHandler.indexQuery(indexname, query, None)}
  future.onSuccess({
    case res =>
      tracker.synchronized {
        if(tracker.status == ProgressiveQueryStatus.RUNNING){
         onComplete(tracker.status, res, confidence, name, info)
        }
       tracker.notifyCompletion(this, res)
      }
  })

  lazy val confidence: Float = Index.confidence(indexname)
}

/**
  * Scan future for sequential scan.
  *
  * @param entityname
  * @param query
  * @param onComplete
  * @param tracker
  */
class SequentialScanFuture(entityname : EntityName, query : NearestNeighbourQuery, onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, String, Map[String, String]) => Unit, val tracker : ProgressiveQueryStatusTracker) extends ScanFuture(tracker) {
  tracker.register(this)

  val name = "sequential"
  val info =  Map[String,String]("type" -> "sequential", "relation" -> entityname, "qid" -> query.queryID.get)

  val future = Future {NearestNeighbourQueryHandler.sequential(entityname, query, None)}
  future.onSuccess({
    case res =>
      tracker.synchronized {
        if (tracker.status == ProgressiveQueryStatus.RUNNING) {
          onComplete(tracker.status, res, confidence, name, info)
        }
        tracker.notifyCompletion(this, res)
      }
  })

  val confidence: Float = 1.toFloat
}