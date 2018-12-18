package org.vitrivr.adampro.communication.rpc

import io.grpc.stub.StreamObserver
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.communication.api.{EntityOp, IndexOp, QueryOp}
import org.vitrivr.adampro.utils.exception.{GeneralAdamException, QueryNotCachedException}
import org.vitrivr.adampro.grpc.grpc.{AdamSearchGrpc, _}
import org.vitrivr.adampro.process.{SharedComponentContext, SparkStartup}
import org.vitrivr.adampro.query.ast.generic.QueryExpression
import org.vitrivr.adampro.query.execution.ProgressiveObservation
import org.vitrivr.adampro.query.execution.parallel.{QueryHintsParallelPathChooser, SimpleParallelPathChooser}
import org.vitrivr.adampro.query.planner.QueryPlannerOp
import org.vitrivr.adampro.query.query.QueryHints
import org.vitrivr.adampro.query.tracker.{QueryTracker, ResultTracker}
import org.vitrivr.adampro.utils.Logging

import scala.concurrent.Future
import scala.util.{Failure, Random, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class DataQuery extends AdamSearchGrpc.AdamSearch with Logging {
  implicit def ac: SharedComponentContext = SparkStartup.mainContext

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
      val res = EntityOp.cache(request.entity)

      if (res.isSuccess) {
        Future.successful(AckMessage(code = AckMessage.Code.OK, request.entity))
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
  override def preview(request: PreviewMessage): Future[QueryResultsMessage] = {
    time("rpc call to preview entity") {
      val res = if (request.n > 0) {
        EntityOp.preview(request.entity, request.n)
      } else {
        EntityOp.preview(request.entity)
      }

      if (res.isSuccess) {
        Future.successful(QueryResultsMessage(Some(AckMessage(AckMessage.Code.OK)), Seq((MessageParser.prepareResults("", 1.toFloat, 0, "sequential scan", Map(), Some(res.get))))))
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
  private def executeQuery(request: QueryMessage, tracker : QueryTracker = new QueryTracker()): QueryResultsMessage = {
    time("rpc call for query operation") {
      val res = runQuery(request, tracker)

      val message = if (res.isSuccess) {
        val finalExpr = res.get._1
        val finalResult = res.get._2

        val informationLevel = MessageParser.prepareInformationLevel(request.information)

        val results = res.get._1.information(informationLevel).map(res =>
          MessageParser.prepareResults(res.id.getOrElse(""), res.confidence.getOrElse(0), res.time.toMillis, res.source.getOrElse(""), Map(), res.results)
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
    * @param tracker
    * @return
    */
  private def runQuery(request: QueryMessage, tracker : QueryTracker = new QueryTracker()): Try[(QueryExpression, Option[DataFrame])] = {
    val logId = Random.alphanumeric.take(5).mkString
    log.trace(QUERY_MARKER, "start " + logId)
    val expression = MessageParser.toExpression(request)
    log.trace(QUERY_MARKER, "to expression " + logId)
    val evaluationOptions = MessageParser.prepareEvaluationOptions(request)
    log.trace(QUERY_MARKER, "evaluation options " + logId)
    val informationLevel = MessageParser.prepareInformationLevel(request.information)
    log.trace(QUERY_MARKER, "information level " + logId)

    if (expression.isFailure) {
      log.error("error when parsing expression: " + expression.failed.get.getMessage)
      return Failure(expression.failed.get)
    }

    log.trace("\n ------------------- \n" + expression.get.mkString(0) + "\n ------------------- \n")

    log.trace(QUERY_MARKER, "before query op " + logId)

    val res = QueryOp.expression(expression.get, evaluationOptions)(tracker)

    log.trace(QUERY_MARKER, "after query op " + logId)

    res
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
        try {
          responseObserver.onNext(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = throwable.getMessage))))
        } catch {
          case e : Exception => log.error(QUERY_MARKER, "failed to inform client about error")
        }
      }

      override def onCompleted(): Unit = {
        responseObserver.onCompleted()
      }

      override def onNext(request: QueryMessage): Unit = {
        object AllDone extends Exception { }

        val res = runQuery(request)

        log.trace(QUERY_MARKER, "run streaming query")

        if(res.isSuccess && res.get._2.isDefined){

          log.trace(QUERY_MARKER, "streaming query is success")

          try {
            val queryInfo = res.get._1.info

            val df = res.get._2.get
            val cols = df.schema

            val zippedRDD = df.rdd.zipWithIndex()

            (0 until MessageParser.MAX_RESULTS by MessageParser.STEP_SIZE).iterator.foreach { paginationStart =>
              log.trace(QUERY_MARKER, "collect streaming results from " + paginationStart + " until " + (paginationStart + MessageParser.STEP_SIZE))
              val subResults = zippedRDD.collect { case (r, i) if i >= paginationStart && i < (paginationStart + MessageParser.STEP_SIZE) => r }.collect()

              if(subResults.isEmpty){
                throw AllDone //get out of loop
              }

              val resMessages = MessageParser.prepareResultsMessages(cols, subResults)
              val queryResultInfoMessage = QueryResultInfoMessage(Some(AckMessage(AckMessage.Code.OK)), queryInfo.id.getOrElse(""), queryInfo.confidence.map(_.toDouble).getOrElse(0.0), queryInfo.time.toMillis, queryInfo.source.getOrElse(""), queryInfo.info, resMessages)

              val queryResultMessage = QueryResultsMessage(
                Some(AckMessage(AckMessage.Code.OK)),
                Seq(queryResultInfoMessage)
              )

              log.trace(QUERY_MARKER, "send streaming results from " + paginationStart + " until " + (paginationStart + MessageParser.STEP_SIZE))
              responseObserver.onNext(queryResultMessage)
            }
          } catch {
            case AllDone => {
              responseObserver.onNext(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.OK, message = "no more results"))))
            }
            case e : Exception => {
              log.error(QUERY_MARKER, "error in execution")
              responseObserver.onNext(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))))
            }
          }
        } else if(!res.get._2.isDefined) {
          log.error(QUERY_MARKER, "error in streaming query execution, successfull execution, but no results")
          responseObserver.onNext(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = "no data to return"))))
        } else {
          log.error(QUERY_MARKER, "error in streaming query execution")
          responseObserver.onNext(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
        }

        log.trace(QUERY_MARKER, "completed streaming query")
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
      Future.successful(BatchedQueryResultsMessage(request.queries.par.map(executeQuery(_)).toList))
    }
  }


  /**
    *
    * @param request
    * @param responseObserver
    */
  override def doParallelQuery(request: QueryMessage, responseObserver: StreamObserver[QueryResultsMessage]): Unit = {
    time("rpc call for parallel query operation") {
      try {
        //track on next
        val onNext =
          (tpo: Try[ProgressiveObservation]) => {
            if (tpo.isSuccess) {
              val po = tpo.get
              responseObserver.onNext(
                QueryResultsMessage(Some(AckMessage(AckMessage.Code.OK)),
                  Seq(MessageParser.prepareResults(request.queryid, po.confidence, po.t2 - po.t1, po.source, po.info, po.results))))
            } else {
              responseObserver.onNext(
                QueryResultsMessage(Some(AckMessage(AckMessage.Code.ERROR, tpo.failed.get.getMessage))))
            }
          }

        val pathChooser = if (request.hints.isEmpty) {
          new SimpleParallelPathChooser()
        } else {
          new QueryHintsParallelPathChooser(request.hints.map(QueryHints.withName(_).get))
        }

        val nnq = if (request.nnq.isDefined) {
          MessageParser.prepareNNQ(request.nnq.get).get
        } else {
          throw new GeneralAdamException("nearest neighbour query necessary for parallel query")
        }
        val bq = if (request.bq.isDefined) {
          Some(MessageParser.prepareBQ(request.bq.get).get)
        } else {
          None
        }

        val evaluationOptions = MessageParser.prepareEvaluationOptions(request)

        //TODO: change here, so that we do not need to rely on "getEntity"
        val tracker = new QueryTracker()
        val pqtracker = QueryOp.parallel(request.from.get.getEntity, nnq, bq, pathChooser, onNext, evaluationOptions)(tracker)

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
          log.error(e.getMessage)
          responseObserver.onNext(QueryResultsMessage(ack = Some(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))))
        }
      }
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
        val onNext =
          (tpo: Try[ProgressiveObservation]) => {
            if (tpo.isSuccess) {
              val po = tpo.get
              responseObserver.onNext(
                QueryResultsMessage(Some(AckMessage(AckMessage.Code.OK)),
                  Seq(MessageParser.prepareResults(request.queryid, po.confidence, po.t2 - po.t1, po.source, po.info, po.results))))
            } else {
              responseObserver.onNext(
                QueryResultsMessage(Some(AckMessage(AckMessage.Code.ERROR, tpo.failed.get.getMessage))))
            }
          }

        val informationLevel = MessageParser.prepareInformationLevel(request.information).head
        val tracker = new QueryTracker(Some(ResultTracker(onNext, informationLevel)))

        val res = executeQuery(request, tracker)
        responseObserver.onCompleted()

        tracker.cleanAll()
      } catch {
        case e: Exception => {
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
      val res = ac.cacheManager.getQuery(request.queryid)

      if (res.isSuccess) {
        Future.successful(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.OK)), Seq(MessageParser.prepareResults(request.queryid, 0, 0, "cache", Map(), Some(res.get)))))
      } else {
        log.error(res.failed.get.getMessage, res.failed.get)
        Future.failed(QueryNotCachedException())
      }
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def getScoredExecutionPath(request: QuerySimulationMessage): Future[ScoredExecutionPathsMessage] = {
    time("rpc call to get scored execution path") {
      try {
        val scans = QueryPlannerOp.scoredScans(MessageParser.getOptimizerName(request.optimizer), request.entity, MessageParser.prepareNNQ(request.nnq.get).get)(None)(ac)

        val res = scans
          .map(ep => {
            ep.expr.rewrite()

            val score = ep.score
            val scan = ep.scan
            val scantype = ep.scantype
            val description = ep.expr.mkString()

            ScoredExecutionPathMessage(score, scan, scantype, description)
          })


        Future.successful(ScoredExecutionPathsMessage(ack = Some(AckMessage(code = AckMessage.Code.OK)), res))
      } catch {
        case e: Exception => {
          log.error(e.getMessage)
          Future.successful(ScoredExecutionPathsMessage(ack = Some(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))))
        }
      }
    }
  }

  override def stopQuery(request: StopQueryMessage): Future[AckMessage] = {
    time("rpc call to stop query") {
      ac.sc.cancelJobGroup(request.jobid)
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    }
  }

  override def stopAllQueries(request: EmptyMessage): Future[AckMessage] = {
    time("rpc call to stop all query") {
      ac.sc.cancelAllJobs()
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    }
  }

  override def ping(request: EmptyMessage): Future[AckMessage] = {
    time("ping"){
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    }
  }
}
