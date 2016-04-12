package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.api.QueryOp
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.index.Index
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
  override def cacheIndex(request: IndexNameMessage): Future[AckMessage] = {
    log.debug("rpc call to cache index")
    try {
      Index.load(request.index, true)
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } catch {
      case e: Exception =>
        log.debug("exception while rpc call for indexing operation")
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def cacheEntity(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call to cache entity")
    log.error("caching entity not yet implemented")
    Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = "not implemented yet"))
  }

  /**
    *
    * @param request
    * @return
    */
  override def doStandardQuery(request: SimpleQueryMessage): Future[QueryResponseListMessage] = {
    log.debug("rpc call for standard query operation")
    try {
      Future.successful(prepareResults(QueryOp.apply(SearchRPCMethods.toQueryHolder(request))))
    } catch {
      case e: Exception => {
        log.error(e)
        Future.failed(e)
      }
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
      Future.successful(prepareResults(QueryOp.sequential(SearchRPCMethods.toQueryHolder(request))))
    } catch {
      case e: Exception => {
        log.error(e)
        Future.failed(e)
      }
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
      Future.successful(prepareResults(QueryOp.index(SearchRPCMethods.toQueryHolder(request))))
    } catch {
      case e: Exception => {
        log.error(e)
        Future.failed(e)
      }
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
      Future.successful(prepareResults(QueryOp.index(SearchRPCMethods.toQueryHolder(request))))
    } catch {
      case e: Exception => {
        log.error(e)
        Future.failed(e)
      }
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
      val onComplete =
        (status: ProgressiveQueryStatus.Value, results: DataFrame, confidence: Float, deliverer: String, info: Map[String, String]) => ({
          responseObserver.onNext(QueryResponseInfoMessage(confidence = confidence, indextype = IndexTypes.withName(info.getOrElse("type", "")).get.indextype, queryResponseList = Option(prepareResults(results))))
        })

      QueryOp.progressive(SearchRPCMethods.toQueryHolder(request, onComplete))
    } catch {
      case e: Exception => {
        log.error(e)
        Future.failed(e)
      }
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
      val (results, confidence, deliverer) = QueryOp.timedProgressive(SearchRPCMethods.toQueryHolder(request))
      Future.successful(QueryResponseInfoMessage(confidence = confidence, indextype = IndexTypes.withName(deliverer).get.indextype, queryResponseList = Option(prepareResults(results))))
    } catch {
      case e: Exception => {
        log.error(e)
        Future.failed(e)
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def doCompoundQuery(request: CompoundQueryMessage): Future[CompoundQueryResponseListMessage] = {
    log.debug("rpc call for chained query operation")

    try {
      val qh = SearchRPCMethods.toQueryHolder(request)

      val results = QueryOp.compoundQuery(qh)
      val intermediate = qh.provideRunInfo().map(res =>
        QueryResponseInfoMessage(id = res.id, time = res.time.toMillis, queryResponseList = Option(prepareResults(res.results)))
      )

      Future.successful(CompoundQueryResponseListMessage(
        intermediate,
        Option(prepareResults(results)))
      )
    } catch {
      case e: Exception => {
        log.error(e)
        Future.failed(e)
      }
    }
  }


  /**
    *
    * @param df
    * @return
    */
  private def prepareResults(df: DataFrame): QueryResponseListMessage = {
    import org.apache.spark.sql.functions.{array, col, lit, udf}

    val asMap = udf((keys: Seq[String], values: Seq[String]) =>
      keys.zip(values).filter {
        case (k, null) => false
        case _ => true
      }.toMap)

    val cols = df.dtypes.slice(2, df.dtypes.length).map(_._1)

    val keys = array(cols.map(lit): _*)
    val values = array(cols.map(col): _*)

    val responseMsgs = if (!cols.isEmpty) {
      df.withColumn("metadata", asMap(keys, values))
        .collect().map(row => QueryResponseMessage(
        row.getAs[Long](FieldNames.idColumnName),
        row.getAs[Float](FieldNames.distanceColumnName),
        row.getMap[String, String](3).toMap
      ))
    } else {
      df
        .collect().map(row => QueryResponseMessage(
        row.getAs[Long](FieldNames.idColumnName),
        row.getAs[Float](FieldNames.distanceColumnName),
        Map[String, String]()
      ))
    }

    QueryResponseListMessage(responseMsgs)
  }
}
