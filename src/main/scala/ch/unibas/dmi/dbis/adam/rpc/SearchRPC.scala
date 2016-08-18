package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.api.{EntityOp, IndexOp, QueryOp}
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.exception.{GeneralAdamException, QueryNotCachedException}
import ch.unibas.dmi.dbis.adam.main.{SparkStartup, AdamContext}
import ch.unibas.dmi.dbis.adam.query.QueryLRUCache
import ch.unibas.dmi.dbis.adam.query.handler.internal.QueryHints
import ch.unibas.dmi.dbis.adam.query.progressive.{ProgressiveObservation, QueryHintsProgressivePathChooser, SimpleProgressivePathChooser}
import io.grpc.stub.StreamObserver
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.vitrivr.adam.grpc.grpc._

import scala.concurrent.Future
import scala.util.Try

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
  override def preview(request: EntityNameMessage): Future[QueryResultsMessage] = {
    time("rpc call to preview entity") {
      val res = EntityOp.preview(request.entity, 100)

      if (res.isSuccess) {
        Future.successful(QueryResultsMessage(Some(AckMessage(AckMessage.Code.OK)), Seq((prepareResults("", 1.toFloat, 0, "sequential scan", Map(), Some(res.get))))))
      } else {
        log.error(res.failed.get.getMessage, res.failed.get)
        Future.successful(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doQuery(request: QueryMessage): Future[QueryResultsMessage] = {
    time("rpc call for query operation") {
      val expression = RPCHelperMethods.toExpression(request)
      val evaluationOptions = RPCHelperMethods.prepareEvaluationOptions(request)
      val informationLevel = RPCHelperMethods.prepareInformationLevel(request.information)

      if (expression.isFailure) {
        Future.successful(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = expression.failed.get.getMessage))))
      }

      val res = QueryOp(expression.get, evaluationOptions)

      log.debug("\n ------------------- \n" + expression.get.mkString(0) + "\n ------------------- \n")

      if (res.isSuccess) {
        val results = expression.get.information(informationLevel).map(res =>
          prepareResults(res.id.getOrElse(""), res.confidence.getOrElse(0), res.time.toMillis, res.source.getOrElse(""), Map(), res.results)
        )

        Future.successful(QueryResultsMessage(
          Some(AckMessage(AckMessage.Code.OK)),
          results
        ))
      } else {
        log.error(res.failed.get.getMessage, res.failed.get)
        Future.successful(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
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
        val onComplete =
          (tpo: Try[ProgressiveObservation]) => {
            if (tpo.isSuccess) {
              val po = tpo.get
              responseObserver.onNext(
                QueryResultsMessage(Some(AckMessage(AckMessage.Code.OK)),
                  Seq(prepareResults(request.queryid, po.confidence, po.t2 - po.t1, po.source, po.info, po.results))))
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
        val tracker = QueryOp.progressive(request.from.get.getEntity, nnq, bq, pathChooser, onComplete, evaluationOptions)

        //track on completed
        while (!tracker.get.isCompleted) {
          Thread.sleep(1000)
        }

        if (tracker.get.isCompleted) {
          responseObserver.onCompleted()
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
      val res = QueryLRUCache.get(request.queryid)

      if (res.isSuccess) {
        Future.successful(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.OK)), Seq(prepareResults(request.queryid, 0, 0, "cache", Map(), Some(res.get)))))
      } else {
        log.error(res.failed.get.getMessage, res.failed.get)
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
    * @param info
    * @param df
    * @return
    */
  private def prepareResults(queryid: String, confidence: Float, time: Long, source: String, info: Map[String, String], df: Option[DataFrame]): QueryResultInfoMessage = {
    val results: Seq[QueryResultTupleMessage] = if (df.isDefined) {
      val cols = df.get.schema

      df.get.collect().map(row => {
        val metadata = cols.map(col => {
          try {
            col.name -> {
              col.dataType match {
                case BooleanType => DataMessage().withBooleanData(row.getAs[Boolean](col.name))
                case DoubleType => DataMessage().withDoubleData(row.getAs[Double](col.name))
                case FloatType => DataMessage().withFloatData(row.getAs[Float](col.name))
                case IntegerType => DataMessage().withIntData(row.getAs[Integer](col.name))
                case LongType => DataMessage().withLongData(row.getAs[Long](col.name))
                case StringType => DataMessage().withStringData(row.getAs[String](col.name))
                case _: FeatureVectorWrapperUDT => DataMessage().withFeatureData(FeatureVectorMessage().withDenseVector(DenseVectorMessage(row.getAs[FeatureVectorWrapper](col.name).toSeq)))
                case _ => DataMessage().withStringData("")
              }
            }
          } catch {
            case e: Exception => col.name -> DataMessage().withStringData("")
          }
        }).toMap

        QueryResultTupleMessage(metadata)
      })
    } else {
      Seq()
    }

    QueryResultInfoMessage(Some(AckMessage(AckMessage.Code.OK)), queryid, confidence, time, source, info, results)
  }
}
