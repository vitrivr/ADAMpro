package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.api.QueryOp
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.http.grpc.adam._
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import io.grpc.stub.StreamObserver
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class SearchRPC extends AdamSearchGrpc.AdamSearch {
  val log = Logger.getLogger(getClass.getName)

  /**
    *
    * @param request
    * @return
    */
  override def doStandardQuery(request: SimpleQueryMessage): Future[QueryResponseListMessage] = {
    log.debug("rpc call for standard query operation")

    try {
        val entity = request.entity
        val hint = QueryHints.withName(request.hint)

        if(!request.nnq.isDefined) {
          throw new Exception("No kNN query specified.")
        }

        val rnnq = request.nnq.get
        val nnq = NearestNeighbourQuery(rnnq.query, NormBasedDistanceFunction(rnnq.norm), rnnq.k, rnnq.indexOnly, rnnq.options)

        val bq : Option[BooleanQuery] = if(!request.bq.isEmpty){
          val rbq = request.bq.get
          Option(BooleanQuery(rbq.where, Option(rbq.joins.map(x => (x.table, x.columns)))))
        } else { None }

        //TODO: metadata should be set via protobuf message
        //TODO: metadata output should be in json
        val results = QueryOp(entity, hint, nnq, bq, true).collect()
          .map(result => QueryResponseMessage(result.getLong(1), result.getDouble(0), ""))

        Future.successful(QueryResponseListMessage(results))
      } catch {
        case e: Exception => Future.failed(e)
      }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doSequentialQuery(request: SimpleSequentialQueryMessage): Future[QueryResponseListMessage] = {
    log.debug("rpc call for sequential query operation")

    try {
      val entity = request.entity

      if(!request.nnq.isDefined) {
        throw new Exception("No kNN query specified.")
      }

      val rnnq = request.nnq.get
      val nnq = NearestNeighbourQuery(rnnq.query, NormBasedDistanceFunction(rnnq.norm), rnnq.k, rnnq.indexOnly, rnnq.options)

      val bq : Option[BooleanQuery] = if(!request.bq.isEmpty){
        val rbq = request.bq.get
        Option(BooleanQuery(rbq.where, Option(rbq.joins.map(x => (x.table, x.columns)))))
      } else { None }

      //TODO: metadata should be set via protobuf message
      //TODO: metadata output should be in json
      val results = QueryOp.sequential(entity, nnq, bq, true).map(result => QueryResponseMessage(result.getLong(1), result.getDouble(0), "")).collect()

      Future.successful(QueryResponseListMessage(results))
    } catch {
      case e: Exception => Future.failed(e)
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doIndexQuery(request: SimpleIndexQueryMessage): Future[QueryResponseListMessage] = {
    log.debug("rpc call for index query operation")

    try {
      val index = request.index

      if(!request.nnq.isDefined) {
        throw new Exception("No kNN query specified.")
      }

      val rnnq = request.nnq.get
      val nnq = NearestNeighbourQuery(rnnq.query, NormBasedDistanceFunction(rnnq.norm), rnnq.k, rnnq.indexOnly, rnnq.options)

      val bq : Option[BooleanQuery] = if(!request.bq.isEmpty){
        val rbq = request.bq.get
        Option(BooleanQuery(rbq.where, Option(rbq.joins.map(x => (x.table, x.columns)))))
      } else { None }

      //TODO: metadata should be set via protobuf message
      //TODO: metadata output should be in json
      val results = QueryOp.index(index, nnq, bq, true).map(result => QueryResponseMessage(result.getLong(1), result.getDouble(0), "")).collect()

      Future.successful(QueryResponseListMessage(results))
    } catch {
      case e: Exception => Future.failed(e)
    }
  }


  /**
    *
    * @param request
    * @param responseObserver
    */
  override def doProgressiveQuery(request: SimpleQueryMessage, responseObserver: StreamObserver[QueryResponseInfoMessage]): Unit = {
    log.debug("rpc call for progressive query operation")

    try {
      val entity = request.entity

      if(!request.nnq.isDefined) {
        throw new Exception("No kNN query specified.")
      }

      val rnnq = request.nnq.get
      val nnq = NearestNeighbourQuery(rnnq.query, NormBasedDistanceFunction(rnnq.norm), rnnq.k, rnnq.indexOnly, rnnq.options)

      val bq : Option[BooleanQuery] = if(!request.bq.isEmpty){
        val rbq = request.bq.get
        Option(BooleanQuery(rbq.where, Option(rbq.joins.map(x => (x.table, x.columns)))))
      } else { None }

      val onComplete =
        (status : ProgressiveQueryStatus.Value, results : DataFrame, confidence : Float, info : Map[String, String]) => ({
          val responseList = QueryResponseListMessage(results.map(result => QueryResponseMessage(result.getLong(1), result.getDouble(0), "")).collect())
          responseObserver.onNext(QueryResponseInfoMessage(confidence, Option(responseList)))
        })
    } catch {
      case e: Exception => Future.failed(e)
    }
  }




  /**
    *
    * @param request
    * @return
    */
  override def doTimedProgressiveQuery(request: TimedQueryMessage): Future[QueryResponseInfoMessage] = {
    log.debug("rpc call for timed progressive query operation")

    try {
      val entity = request.entity
      val time = request.time

      if(!request.nnq.isDefined) {
        throw new Exception("No kNN query specified.")
      }

      val rnnq = request.nnq.get
      val nnq = NearestNeighbourQuery(rnnq.query, NormBasedDistanceFunction(rnnq.norm), rnnq.k, rnnq.indexOnly, rnnq.options)

      val bq : Option[BooleanQuery] = if(!request.bq.isEmpty){
        val rbq = request.bq.get
        Option(BooleanQuery(rbq.where, Option(rbq.joins.map(x => (x.table, x.columns)))))
      } else { None }

      //TODO: metadata should be set via protobuf message
      //TODO: metadata output should be in json
      val tpresults = QueryOp.timedProgressive(entity, nnq, bq, Duration(time, TimeUnit.MILLISECONDS), true)

      val results = tpresults._1.map(result => QueryResponseMessage(result.getLong(1), result.getDouble(0), "")).collect()
      val confidence = tpresults._2

      Future.successful(QueryResponseInfoMessage(confidence, Option(QueryResponseListMessage(results))))
    } catch {
      case e: Exception => Future.failed(e)
    }
  }
}
