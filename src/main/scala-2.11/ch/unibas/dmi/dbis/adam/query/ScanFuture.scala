package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table.TableName

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */


abstract class ScanFuture(tracker : ProgressiveQueryStatusTracker){
  val queryID :  String
  val future : Future[_]

  val confidence : Float
}


class IndexScanFuture(indexname : IndexName, q : WorkingVector, distance : NormBasedDistanceFunction, k : Int, options : Map[String, String], onComplete: (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit, val queryID :  String, val tracker : ProgressiveQueryStatusTracker) extends ScanFuture(tracker) {
  tracker.register(this)

  val info =  Map[String,String]("type" -> ("index: " + indexname), "index" -> indexname, "qid" -> queryID)

  val future = Future {QueryHandler.indexQuery(q, distance, k, indexname, options.toMap, None, Some(queryID))}
  future.onSuccess({
    case res =>
      tracker.synchronized {
        if(tracker.getStatus() == ProgressiveQueryStatus.RUNNING){
         onComplete(tracker.getStatus, res, confidence, info)
        }
       tracker.notifyCompletion(this, res)
      }
  })

  lazy val confidence: Float = Index.retrieveIndexConfidence(indexname)
}


class SequentialScanFuture(tablename : TableName, q : WorkingVector, distance : NormBasedDistanceFunction, k : Int,  onComplete: (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit, val queryID :  String, val tracker : ProgressiveQueryStatusTracker) extends ScanFuture(tracker) {
  tracker.register(this)

  val info =  Map[String,String]("type" -> "sequential", "relation" -> tablename, "qid" -> queryID)

  val future = Future {QueryHandler.sequentialQuery(q, distance, k, tablename, None, Some(queryID))}
  future.onSuccess({
    case res =>
      tracker.synchronized {
        if (tracker.getStatus() == ProgressiveQueryStatus.RUNNING) {
          onComplete(tracker.getStatus, res, confidence, info)
        }
        tracker.notifyCompletion(this, res)
      }
  })

  val confidence: Float = 1.toFloat
}