package ch.unibas.dmi.dbis.adam.query

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.progressive.{IndexScanFuture, ProgressiveQueryStatus, ProgressiveQueryStatusTracker, SequentialScanFuture}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import org.apache.spark.Logging

import scala.collection.immutable.HashSet
import scala.collection.mutable.{Map => mMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object QueryHandler extends Logging {
  private val metadataStorage = SparkStartup.metadataStorage
  private val featureStorage = SparkStartup.featureStorage

  /**
   *
   * @param where
   * @param entityname
   * @return
   */
  def metadataQuery(where: Map[String, String], entityname: EntityName): HashSet[TupleID] = {
    val filter = where.map(c => c._1 + " = " + c._2).mkString(" AND ")
    val res = metadataStorage.read(entityname).filter(filter).map(r => r.getLong(0)).collect()
    HashSet(res : _*)
  }


  /**
   *
   * @param where
   * @param entityname
   * @return
   */
  def metadataQuery(where: String, entityname: EntityName): HashSet[TupleID] = {
    val res = metadataStorage.read(entityname).filter(where).map(r => r.getLong(0)).collect()
    HashSet(res : _*)
  }


  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param entityname
   * @return
   */
  def sequentialQuery(q: FeatureVector, distance: DistanceFunction, k: Int, entityname: EntityName, filter: Option[HashSet[TupleID]], queryID : Option[String] = None): Seq[Result] =
    FeatureScanner(Entity.retrieveEntity(entityname), q, distance, k, filter, queryID)

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param indexname
   * @param options
   * @return
   */
  def indexQuery(q: FeatureVector, distance: DistanceFunction, k: Int, indexname: IndexName, options: Map[String, String], filter: Option[HashSet[TupleID]], queryID : Option[String] = None): Seq[Result] = {
    val onlyIndexResults = options.getOrElse("onlyindex", "false").toBoolean

    if (!onlyIndexResults) {
      indexAndFeatureScan(q, distance, k, indexname, options, filter, queryID)
    } else {
      indexScanOnly(q, distance, k, indexname, options, filter, queryID)
    }
  }

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param indexname
   * @param options
   * @return
   */
  def indexAndFeatureScan(q: FeatureVector, distance: DistanceFunction, k: Int, indexname: IndexName, options: Map[String, String], filter: Option[HashSet[TupleID]], queryID : Option[String] = None): Seq[Result] = {
    val entityname = CatalogOperator.getIndexEntity(indexname)

    val future = Future {
      Entity.retrieveEntity(entityname)
    }

    val tidList = IndexScanner(q, distance, k, indexname, options, filter, queryID)

    val entity = Await.result[Entity](future, Duration(100, TimeUnit.SECONDS))
    FeatureScanner(entity, q, distance, k, Some(tidList), queryID)
  }

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param indexname
   * @param options
   * @return
   */
  def indexScanOnly(q: FeatureVector, distance: DistanceFunction, k: Int, indexname: IndexName, options: Map[String, String], filter: Option[HashSet[TupleID]], queryID : Option[String] = None): Seq[Result] =
    IndexScanner(q, distance, k, indexname, options, filter).toList.map(tid => Result(-1, tid, null))


  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param entityname
   */
  def progressiveQuery(q: FeatureVector, distance: DistanceFunction, k: Int, entityname: EntityName, filter: Option[HashSet[TupleID]], onComplete: (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit, queryID : Option[String] = Some(java.util.UUID.randomUUID().toString)): Int = {
    val indexnames = Index.getIndexnames(entityname)

    val options = mMap[String, String]()

    val tracker = new ProgressiveQueryStatusTracker(queryID.get)

    //index scans
    val indexScanFutures = indexnames.par.map { indexname =>
      val isf = new IndexScanFuture(indexname, q, distance, k, options.toMap, onComplete, queryID.get, tracker)
    }

    //sequential scan
    val ssf = new SequentialScanFuture(entityname, q, distance, k, onComplete, queryID.get, tracker)


    //number of queries running (indexes + sequential)
    indexnames.length + 1
  }


  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param entityname
   */
  def timedProgressiveQuery(q: FeatureVector, distance: DistanceFunction, k: Int, entityname: EntityName, filter: Option[HashSet[TupleID]], timelimit : Duration, queryID : Option[String] = Some(java.util.UUID.randomUUID().toString)): (Seq[Result], Float) = {
    val indexnames = Index.getIndexnames(entityname)

    val options = mMap[String, String]()

    val tracker = new ProgressiveQueryStatusTracker(queryID.get)

    val timerFuture = Future{Thread.sleep(timelimit.toMillis)}

    //index scans
    val indexScanFutures = indexnames.par.map { indexname =>
      val isf = new IndexScanFuture(indexname, q, distance, k, options.toMap, (status, result, confidence, info) => (), queryID.get, tracker)
    }

    //sequential scan
    val ssf = new SequentialScanFuture(entityname, q, distance, k, (status, result, confidence, info) => (), queryID.get, tracker)

    Await.result(timerFuture, timelimit)

    tracker.results
  }
}


