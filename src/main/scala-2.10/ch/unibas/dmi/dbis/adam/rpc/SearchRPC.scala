package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import io.grpc.stub.StreamObserver
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.concurrent.Future

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
      Future.successful(prepareResults(SearchRPCMethods.runStandardQuery(request)))
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
      Future.successful(prepareResults(SearchRPCMethods.runSequentialQuery(request)))
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
      Future.successful(prepareResults(SearchRPCMethods.runIndexQuery(request)))
    } catch {
      case e: Exception => Future.failed(e)
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doSpecifiedIndexQuery(request: SimpleSpecifiedIndexQueryMessage): Future[QueryResponseListMessage] = {
    log.debug("rpc call for index query operation")

    try {
      Future.successful(prepareResults(SearchRPCMethods.runSpecifiedIndexQuery(request)))
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
      val onComplete: (ProgressiveQueryStatus.Value, DataFrame, VectorBase, String, Map[String, String]) => Unit =
        (status: ProgressiveQueryStatus.Value, results: DataFrame, confidence: Float, deliverer: String, info: Map[String, String]) => ({
          responseObserver.onNext(QueryResponseInfoMessage(confidence, IndexTypes.withName(info.getOrElse("type", "")).get.indextype, Option(prepareResults(results))))
        })

      SearchRPCMethods.runProgressiveQuery(request, onComplete)
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
      val (results, confidence, deliverer) = SearchRPCMethods.runTimedProgressiveQuery(request)
      Future.successful(QueryResponseInfoMessage(confidence, IndexTypes.withName(deliverer).get.indextype, Option(prepareResults(results))))
    } catch {
      case e: Exception => Future.failed(e)
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doCompoundQuery(request: ExpressionQueryMessage): Future[QueryResponseListMessage] = {
    log.debug("rpc call for chained query operation")

    try {
      Future.successful(prepareResults(CompoundSearchRPCMethods.runCompoundQuery(request)))
    } catch {
      case e: Exception => Future.failed(e)
    }
  }


  /**
    *
    * @param df
    * @return
    */
  private def prepareResults(df: DataFrame): QueryResponseListMessage = {
    import org.apache.spark.sql.functions.{col, concat, concat_ws, lit}

    val responseMsgs = df.select(
      df(FieldNames.idColumnName),
      df(FieldNames.distanceColumnName),
      concat(//creates JSON
        lit("{"),
        concat_ws(",", df.dtypes.slice(2, df.dtypes.length).map(dt => {
          val c = dt._1;
          val t = dt._2;
          concat(
            lit("\"" + c + "\":" + (if (t == "StringType") "\""; else "")),
            col(c),
            lit(if (t == "StringType") "\""; else "")
          )
        }): _*),
        lit("}")
      ) as "metadata"
    ).collect().map(row => QueryResponseMessage(
      row.getAs[Long](FieldNames.idColumnName),
      row.getAs[Float](FieldNames.distanceColumnName),
      row.getAs[String]("metadata")
    ))

    QueryResponseListMessage(responseMsgs)
  }
}
