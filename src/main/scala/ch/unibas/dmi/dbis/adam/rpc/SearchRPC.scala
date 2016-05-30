package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.api.{IndexOp, QueryOp}
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.exception.QueryNotCachedException
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.QueryLRUCache
import ch.unibas.dmi.dbis.adam.query.handler.internal.QueryHints
import ch.unibas.dmi.dbis.adam.query.progressive.{ProgressiveObservation, QueryHintsProgressivePathChooser, SimpleProgressivePathChooser}
import io.grpc.stub.StreamObserver
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.concurrent.Future

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
  override def doQuery(request: QueryMessage): Future[QueryResultsMessage] = {
    time("rpc call for query operation") {
      val expression = RPCHelperMethods.toExpression(request)

      if (expression.isFailure) {
        Future.successful(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = expression.failed.get.getMessage))))
      }

      val res = QueryOp(expression.get)

      log.debug("\n ------------------- \n" + expression.get.mkString(0) + "\n ------------------- \n")

      if (res.isSuccess) {
        val resultInfos = expression.get.information()

        val results = resultInfos.map(res =>
          prepareResults(res.id.getOrElse(""), res.confidence.getOrElse(0), res.time.toMillis, res.source.getOrElse(""), res.results)
        )

        Future.successful(QueryResultsMessage(
          Some(AckMessage(AckMessage.Code.OK)),
          results
        ))
      } else {
        log.error(res.failed.get.getMessage)
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
          (po: ProgressiveObservation) => ({
            responseObserver.onNext(
              QueryResultsMessage(Some(AckMessage(AckMessage.Code.OK)),
                Seq(prepareResults(request.queryid, po.confidence, po.t2 - po.t1, po.source + " (" + po.info.get("name").getOrElse("no details") + ")", po.results))))
          })

        val pathChooser = if (request.hints.isEmpty) {
          new SimpleProgressivePathChooser()
        } else {
          new QueryHintsProgressivePathChooser(request.hints.map(QueryHints.withName(_).get))
        }

        val tracker = QueryOp.progressive(request.from.get.getEntity, RPCHelperMethods.prepareNNQ(request.nnq).get, RPCHelperMethods.prepareBQ(request.bq), pathChooser, onComplete)

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
        Future.successful(QueryResultsMessage(Some(AckMessage(code = AckMessage.Code.OK)), Seq(prepareResults(request.queryid, 0, 0, "cache", Some(res.get)))))
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
  private def prepareResults(queryid: String, confidence: Float, time: Long, source: String, df: Option[DataFrame]): QueryResultInfoMessage = {
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
                case _ : FeatureVectorWrapperUDT => DataMessage().withFeatureData(FeatureVectorMessage().withDenseVector(DenseVectorMessage(row.getAs[FeatureVectorWrapper](col.name).toSeq)))
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

    QueryResultInfoMessage(Some(AckMessage(AckMessage.Code.OK)), queryid, confidence, time, source, results)
  }
}
