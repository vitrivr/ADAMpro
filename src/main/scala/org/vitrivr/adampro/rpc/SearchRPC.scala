package org.vitrivr.adampro.rpc

import io.grpc.stub.StreamObserver
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.vitrivr.adampro.api.{EntityOp, IndexOp, QueryOp}
import org.vitrivr.adampro.datatypes.gis.{GeographyWrapper, GeometryWrapper}
import org.vitrivr.adampro.datatypes.vector.Vector.{DenseSparkVector, SparseSparkVector}
import org.vitrivr.adampro.datatypes.vector.{DenseVectorWrapper, SparseVectorWrapper}
import org.vitrivr.adampro.exception.{GeneralAdamException, QueryNotCachedException}
import org.vitrivr.adampro.grpc.grpc.{AdamSearchGrpc, _}
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.query.QueryHints
import org.vitrivr.adampro.query.progressive.{ProgressiveObservation, QueryHintsProgressivePathChooser, SimpleProgressivePathChooser}
import org.vitrivr.adampro.utils.Logging

import scala.concurrent.Future
import scala.util.{Random, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class SearchRPC extends AdamSearchGrpc.AdamSearch with Logging {
  implicit def ac: AdamContext = SparkStartup.mainContext

  /**
    *
    * @param thunk
    * @tparam T
    * @return
    */
  def time[T](desc: String)(thunk: => T): T = {
    val t1 = System.currentTimeMillis
    log.trace(desc + " : " + "started at " + t1)
    val x = thunk
    val t2 = System.currentTimeMillis
    log.trace(desc + " : " + "started at " + t1 + " finished at " + t2)
    log.debug(desc + " : " + (t2 - t1) + " msecs")
    x
  }


  /**
    *
    * @param request
    * @return
    */
  override def cacheIndex(request: IndexNameMessage): Future[AckMessage] = {
    time("rpc call to cache index") {
      val res = IndexOp.cache(request.index)

      if (res.isSuccess) {
        Future.successful(AckMessage(code = AckMessage.Code.OK, res.get.entityname))
      } else {
        log.error(res.failed.get.getMessage, res.failed.get)
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def cacheEntity(request: EntityNameMessage): Future[AckMessage] = {
    time("rpc call to cache entity") {
      log.error("caching entity not yet implemented")
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = "not implemented yet"))
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def preview(request: PreviewMessage): Future[QueryResultsMessage] = {
    time("rpc call to preview entity") {
      val res = if (request.n > 0) {
        EntityOp.preview(request.entity, request.n)
      } else {
        EntityOp.preview(request.entity)
      }

      if (res.isSuccess) {
        Future.successful(QueryResultsMessage(Some(AckMessage(AckMessage.Code.OK)), Seq((RPCHelperMethods.prepareResults("", 1.toFloat, 0, "sequential scan", Map(), Some(res.get))))))
      } else {
        log.error(res.failed.get.getMessage, res.failed.get)
        Future.successful(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
      }
    }
  }

  /**
    *
    * @param request
    */
  private def executeQuery(request: QueryMessage): QueryResultsMessage = {
    time("rpc call for query operation") {
      val logId = Random.alphanumeric.take(5).mkString
      log.trace(QUERY_MARKER, "start " + logId)
      val expression = RPCHelperMethods.toExpression(request)
      log.trace(QUERY_MARKER, "to expression " + logId)
      val evaluationOptions = RPCHelperMethods.prepareEvaluationOptions(request)
      log.trace(QUERY_MARKER, "evaluation options " + logId)
      val informationLevel = RPCHelperMethods.prepareInformationLevel(request.information)
      log.trace(QUERY_MARKER, "information level " + logId)

      if (expression.isFailure) {
        log.error("error when parsing expression: " + expression.failed.get.getMessage)
        return QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = expression.failed.get.getMessage)))
      }

      log.trace("\n ------------------- \n" + expression.get.mkString(0) + "\n ------------------- \n")

      log.trace(QUERY_MARKER, "before query op " + logId)

      val tracker = new OperationTracker()
      val res = QueryOp.expression(expression.get, evaluationOptions)(tracker)

      log.trace(QUERY_MARKER, "after query op " + logId)

      val message = if (res.isSuccess) {
        val results = expression.get.information(informationLevel).map(res =>
          RPCHelperMethods.prepareResults(res.id.getOrElse(""), res.confidence.getOrElse(0), res.time.toMillis, res.source.getOrElse(""), Map(), res.results)
        )

        QueryResultsMessage(
          Some(AckMessage(AckMessage.Code.OK)),
          results
        )
      } else {
        log.error(res.failed.get.getMessage, res.failed.get)
        QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage)))
      }

      tracker.cleanAll()

      message
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doQuery(request: QueryMessage): Future[QueryResultsMessage] = {
    time("rpc call for query operation") {
      Future.successful(executeQuery(request))
    }
  }

  /**
    *
    * @param responseObserver
    * @return
    */
  override def doStreamingQuery(responseObserver: StreamObserver[QueryResultsMessage]): StreamObserver[QueryMessage] = {
    return new StreamObserver[QueryMessage]() {
      override def onError(throwable: Throwable): Unit = {
        responseObserver.onNext(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = throwable.getMessage))))
      }

      override def onCompleted(): Unit = {}

      override def onNext(request: QueryMessage): Unit = {
        responseObserver.onNext(executeQuery(request))
      }
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def doBatchQuery(request: BatchedQueryMessage): Future[BatchedQueryResultsMessage] = {
    time("rpc call for batched query operation") {
      Future.successful(BatchedQueryResultsMessage(request.queries.map(executeQuery(_))))
    }
  }


  /**
    *
    * @param request
    * @param responseObserver
    */
  override def doProgressiveQuery(request: QueryMessage, responseObserver: StreamObserver[QueryResultsMessage]): Unit = {
    time("rpc call for progressive query operation") {
      try {
        //track on next
        val onComplete =
          (tpo: Try[ProgressiveObservation]) => {
            if (tpo.isSuccess) {
              val po = tpo.get
              responseObserver.onNext(
                QueryResultsMessage(Some(AckMessage(AckMessage.Code.OK)),
                  Seq(RPCHelperMethods.prepareResults(request.queryid, po.confidence, po.t2 - po.t1, po.source, po.info, po.results))))
            } else {
              responseObserver.onNext(
                QueryResultsMessage(Some(AckMessage(AckMessage.Code.ERROR, tpo.failed.get.getMessage))))
            }
          }

        val pathChooser = if (request.hints.isEmpty) {
          new SimpleProgressivePathChooser()
        } else {
          new QueryHintsProgressivePathChooser(request.hints.map(QueryHints.withName(_).get))
        }

        val nnq = if (request.nnq.isDefined) {
          RPCHelperMethods.prepareNNQ(request.nnq.get).get
        } else {
          throw new GeneralAdamException("nearest neighbour query necessary for progressive query")
        }
        val bq = if (request.bq.isDefined) {
          Some(RPCHelperMethods.prepareBQ(request.bq.get).get)
        } else {
          None
        }

        val evaluationOptions = RPCHelperMethods.prepareEvaluationOptions(request)

        //TODO: change here, so that we do not need to rely on "getEntity"
        val tracker = new OperationTracker()
        val pqtracker = QueryOp.progressive(request.from.get.getEntity, nnq, bq, pathChooser, onComplete, evaluationOptions)(tracker)

        //track on completed
        while (!pqtracker.get.isCompleted) {
          Thread.sleep(1000)
        }

        if (pqtracker.get.isCompleted) {
          responseObserver.onCompleted()
          tracker.cleanAll()
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
          log.error(e.getMessage)
          responseObserver.onNext(QueryResultsMessage(ack = Some(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))))
        }
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def getCachedResults(request: CachedResultsMessage): Future[QueryResultsMessage] = {
    time("rpc call for cached query results") {
      val res = ac.queryLRUCache.get(request.queryid)

      if (res.isSuccess) {
        Future.successful(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.OK)), Seq(RPCHelperMethods.prepareResults(request.queryid, 0, 0, "cache", Map(), Some(res.get)))))
      } else {
        log.error(res.failed.get.getMessage, res.failed.get)
        Future.failed(QueryNotCachedException())
      }
    }
  }


}
