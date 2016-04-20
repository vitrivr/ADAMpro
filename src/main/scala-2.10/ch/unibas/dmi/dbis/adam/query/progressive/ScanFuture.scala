package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.IndexHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{ProgressiveQueryStatus, ProgressiveQueryStatusTracker}
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
abstract class ScanFuture(tracker: ProgressiveQueryStatusTracker) {
  val typename: String
  val future: Future[_]
  val confidence: Float
}

/**
  * Scan future for indexes.
  *
  * @param indexname
  * @param query
  * @param onComplete
  * @param tracker
  */
class IndexScanFuture[U](indexname: IndexName, query: NearestNeighbourQuery, onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, String, Map[String, String]) => U, val tracker: ProgressiveQueryStatusTracker)(implicit ac: AdamContext) extends ScanFuture(tracker) {
  tracker.register(this)
  val index = IndexHandler.load(indexname).get

  val typename = index.indextypename.name
  val info = Map[String, String]("type" -> ("index: " + indexname), "index" -> indexname, "qid" -> query.queryID.get)

  val future = Future {
    NearestNeighbourQueryHandler.indexQuery(index, query, None)
  }
  future.onSuccess({
    case res =>
      tracker.synchronized {
        if (tracker.status == ProgressiveQueryStatus.RUNNING) {
          onComplete(tracker.status, res, confidence, typename, info)
        }
        tracker.notifyCompletion(this, res)
      }
  })

  lazy val confidence: Float = index.confidence
}

/**
  * Scan future for sequential scan.
  *
  * @param entityname
  * @param query
  * @param onComplete
  * @param tracker
  */
class SequentialScanFuture[U](entityname: EntityName, query: NearestNeighbourQuery, onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, String, Map[String, String]) => U, val tracker: ProgressiveQueryStatusTracker)(implicit ac: AdamContext) extends ScanFuture(tracker) {
  tracker.register(this)

  val typename = "sequential"
  val info = Map[String, String]("type" -> "sequential", "relation" -> entityname, "qid" -> query.queryID.get)

  val future = Future {
    NearestNeighbourQueryHandler.sequential(entityname, query, None)
  }
  future.onSuccess({
    case res =>
      tracker.synchronized {
        if (tracker.status == ProgressiveQueryStatus.RUNNING) {
          onComplete(tracker.status, res, confidence, typename, info)
        }
        tracker.notifyCompletion(this, res)
      }
  })

  val confidence: Float = 1.toFloat
}