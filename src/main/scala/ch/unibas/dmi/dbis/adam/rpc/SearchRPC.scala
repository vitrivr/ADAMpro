package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.api.{IndexOp, QueryOp}
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.exception.QueryNotCachedException
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{ProgressiveQueryStatus, QueryLRUCache}
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints
import ch.unibas.dmi.dbis.adam.query.handler.internal.CompoundQueryHolder
import ch.unibas.dmi.dbis.adam.query.progressive.{QueryHintsProgressivePathChooser, SimpleProgressivePathChooser}
import io.grpc.stub.StreamObserver
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class SearchRPC(implicit ac: AdamContext) extends AdamSearchGrpc.AdamSearch with Logging {
  //TODO: possibly start new 'lightweight' AdamContext with each new query

  /**
    *
    * @param thunk
    * @tparam T
    * @return
    */
  def time[T](desc: String)(thunk: => T): T = {
    val t1 = System.currentTimeMillis
    val x = thunk
    val t2 = System.currentTimeMillis
    log.info(desc + " : " + (t2 - t1) + " msecs")
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
        log.error(res.failed.get.getMessage)
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
  override def doStandardQuery(request: SimpleQueryMessage): Future[QueryResponseInfoMessage] = {
    time("rpc call for standard query operation") {
      val expression = RPCHelperMethods.toExpression(request)
      if (expression.isFailure) {
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = expression.failed.get.getMessage))))
      }
      val res = QueryOp(expression.get)

      if (res.isSuccess) {
        Future.successful(prepareResults(request.queryid, 0.0, 0, "", res.get))
      } else {
        log.error(res.failed.get.getMessage)
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doSequentialQuery(request: SimpleSequentialQueryMessage): Future[QueryResponseInfoMessage] = {
    time("rpc call for sequential query operation") {
      val expression = RPCHelperMethods.toExpression(request)
      if (expression.isFailure) {
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = expression.failed.get.getMessage))))
      }
      val res = QueryOp(expression.get)

      if (res.isSuccess) {
        Future.successful(prepareResults(request.queryid, 0.0, 0, "", res.get))
      } else {
        log.error(res.failed.get.getMessage)
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doIndexQuery(request: SimpleIndexQueryMessage): Future[QueryResponseInfoMessage] = {
    time("rpc call for index query operation") {
      val expression = RPCHelperMethods.toExpression(request)
      if (expression.isFailure) {
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = expression.failed.get.getMessage))))
      }
      val res = QueryOp(expression.get)

      if (res.isSuccess) {
        Future.successful(prepareResults(request.queryid, 0.0, 0, "", res.get))
      } else {
        log.error(res.failed.get.getMessage)
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doSpecifiedIndexQuery(request: SimpleSpecifiedIndexQueryMessage): Future[QueryResponseInfoMessage] = {
    time("rpc call for index query operation") {
      val expression = RPCHelperMethods.toExpression(request)
      if (expression.isFailure) {
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = expression.failed.get.getMessage))))
      }
      val res = QueryOp(expression.get)

      if (res.isSuccess) {
        Future.successful(prepareResults(request.queryid, 0.0, 0, "", res.get))
      } else {
        log.error(res.failed.get.getMessage)
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
      }
    }
  }


  /**
    *
    * @param request
    * @param responseObserver
    */
  override def doProgressiveQuery(request: SimpleQueryMessage, responseObserver: StreamObserver[QueryResponseInfoMessage]): Unit = {
    time("rpc call for progressive query operation") {
      try {
        //track on next
        val onComplete =
          (status: ProgressiveQueryStatus.Value, df: DataFrame, confidence: Float, source: String, info: Map[String, String]) => ({
            responseObserver.onNext(prepareResults(request.queryid, confidence, 0, source + " (" + info.get("name").getOrElse("no details") + ")", df))
          })

        val pathChooser = if (request.hints.isEmpty) {
          new SimpleProgressivePathChooser()
        } else {
          new QueryHintsProgressivePathChooser(request.hints.map(QueryHints.withName(_).get))
        }

        val tracker = QueryOp.progressive(request.entity, RPCHelperMethods.prepareNNQ(request.nnq).get, RPCHelperMethods.prepareBQ(request.bq), pathChooser, onComplete, request.withMetadata)

        //track on completed
        while (!tracker.get.isCompleted) {
          Thread.sleep(1000)
        }

        if (tracker.get.isCompleted) {
          responseObserver.onCompleted()
        }
      } catch {
        case e: Exception => {
          log.error(e.getMessage)
          responseObserver.onNext(QueryResponseInfoMessage(ack = Some(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))))
        }
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doTimedProgressiveQuery(request: TimedQueryMessage): Future[QueryResponseInfoMessage] = {
    time ("rpc call for timed progressive query operation") {
      val res = QueryOp.timedProgressive(request.entity, RPCHelperMethods.prepareNNQ(request.nnq).get, RPCHelperMethods.prepareBQ(request.bq), new SimpleProgressivePathChooser(), Duration(request.time, TimeUnit.MILLISECONDS), request.withMetadata)

      if (res.isSuccess) {
        val (df, confidence, source) = res.get
        Future.successful(prepareResults(request.queryid, confidence, 0, source, df))
      } else {
        log.error(res.failed.get.getMessage)
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doCompoundQuery(request: CompoundQueryMessage): Future[CompoundQueryResponseInfoMessage] = {
    time("rpc call for chained query operation") {
      try {
        val expression = RPCHelperMethods.toExpression(request)
        if (expression.isFailure) {
          Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = expression.failed.get.getMessage))))
        }
        val res = QueryOp(expression.get)

        if (res.isSuccess) {
          val resultInfos = if (request.withIntermediateResults) {
            expression.get.asInstanceOf[CompoundQueryHolder].provideRunInfo()
          } else {
            expression.get.asInstanceOf[CompoundQueryHolder].getRunDetails(new ListBuffer())
          }

          val results = resultInfos.map(res =>
            prepareResults(res.id, 0.0, res.time.toMillis, res.source, res.results)
          )

          Future.successful(CompoundQueryResponseInfoMessage(
            Some(AckMessage(AckMessage.Code.OK)),
            results
          ))
        } else {
          log.error(res.failed.get.getMessage)
          Future.successful(CompoundQueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
        }
      } catch {
        case e: Exception => {
          log.error(e.getMessage)
          Future.successful(CompoundQueryResponseInfoMessage(ack = Some(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))))
        }
      }
    }
  }

  override def doBooleanQuery(request: SimpleBooleanQueryMessage): Future[QueryResponseInfoMessage] = {
    time("rpc call for Boolean query operation") {
      val expression = RPCHelperMethods.toExpression(request)
      if (expression.isFailure) {
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = expression.failed.get.getMessage))))
      }

      val res = QueryOp(expression.get)
      if (res.isSuccess) {
        Future.successful(prepareResults(request.queryid, 0.0, 0, "metadata", res.get))
      } else {
        log.error(res.failed.get.getMessage)
        Future.successful(QueryResponseInfoMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def getCachedResults(request: CachedResultsMessage): Future[QueryResponseInfoMessage] = {
    time("rpc call for cached query results") {
      val res = QueryLRUCache.get(request.queryid)

      if (res.isSuccess) {
        Future.successful(prepareResults(request.queryid, 0.0, 0, "cache", res.get))
      } else {
        log.error(res.failed.get.getMessage)
        Future.failed(QueryNotCachedException())
      }
    }
  }

  /**
    *
    * @param queryid
    * @param confidence
    * @param time
    * @param source
    * @param df
    * @return
    */
  private def prepareResults(queryid: String, confidence: Double, time: Long, source: String, df: DataFrame): QueryResponseInfoMessage = {
    val cols = df.schema.map(_.name)

    val results = df.collect().map(row => {
      val distance = row.getAs[Float](FieldNames.distanceColumnName)
      val metadata = cols.map(col => {
        try {
          col -> row.getAs(col).toString
        } catch {
          case e: Exception => col -> ""
        }
      }).toMap
      QueryResultMessage(distance, metadata)
    })

    QueryResponseInfoMessage(Some(AckMessage(AckMessage.Code.OK)), queryid, confidence, time, source, results)
  }
}
