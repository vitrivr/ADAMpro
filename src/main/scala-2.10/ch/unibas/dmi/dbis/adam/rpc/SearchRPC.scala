package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.api.QueryOp
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.exception.QueryNotCachedException
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.index.IndexHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import io.grpc.stub.StreamObserver
import org.apache.log4j.Logger
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
class SearchRPC(implicit ac : AdamContext) extends AdamSearchGrpc.AdamSearch {
  val log = Logger.getLogger(getClass.getName)


  //TODO: possibly start new 'lightweight' AdamContext with each new query

  /**
    *
    * @param request
    * @return
    */
  override def cacheIndex(request: IndexNameMessage): Future[AckMessage] = {
    log.debug("rpc call to cache index")
    try {
      IndexHandler.load(request.index, true)
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
  override def doStandardQuery(request: SimpleQueryMessage): Future[QueryResponseInfoMessage] = {
    log.debug("rpc call for standard query operation")
    try {
      val df = QueryOp.apply(SearchRPCMethods.toQueryHolder(request))
      Future.successful(prepareResults(request.queryid, 0.0, 0, "", df))
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
  override def doSequentialQuery(request: SimpleSequentialQueryMessage): Future[QueryResponseInfoMessage] = {
    log.debug("rpc call for sequential query operation")
    try {
      val df = QueryOp.sequential(SearchRPCMethods.toQueryHolder(request))
      Future.successful(prepareResults(request.queryid, 0.0, 0, "", df))
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
  override def doIndexQuery(request: SimpleIndexQueryMessage): Future[QueryResponseInfoMessage] = {
    log.debug("rpc call for index query operation")

    try {
      val df = QueryOp.index(SearchRPCMethods.toQueryHolder(request))
      Future.successful(prepareResults(request.queryid, 0.0, 0, "", df))
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
  override def doSpecifiedIndexQuery(request: SimpleSpecifiedIndexQueryMessage): Future[QueryResponseInfoMessage] = {
    log.debug("rpc call for index query operation")

    try {
      val df = QueryOp.index(SearchRPCMethods.toQueryHolder(request))
      Future.successful(prepareResults(request.queryid, 0.0, 0, "", df))
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
        (status: ProgressiveQueryStatus.Value, df: DataFrame, confidence: Float, source: String, info: Map[String, String]) => ({
          responseObserver.onNext(prepareResults(request.queryid, confidence, 0, source, df))
        })

        QueryOp.progressive(request.entity, SearchRPCMethods.prepareNNQ(request.nnq), SearchRPCMethods.prepareBQ(request.bq), onComplete, request.withMetadata)
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
      val (df, confidence, source) = QueryOp.timedProgressive(request.entity, SearchRPCMethods.prepareNNQ(request.nnq), SearchRPCMethods.prepareBQ(request.bq), Duration(request.time, TimeUnit.MILLISECONDS), request.withMetadata)
      Future.successful(prepareResults(request.queryid, confidence, 0, source, df))
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
  override def doCompoundQuery(request: CompoundQueryMessage): Future[CompoundQueryResponseInfoMessage] = {
    log.debug("rpc call for chained query operation")

    try {
      val qh = SearchRPCMethods.toQueryHolder(request)

      val finalResults = QueryOp.compoundQuery(qh)

      val resultInfos = if (request.withIntermediateResults) {
        qh.provideRunInfo()
      } else {
        qh.getRunDetails(new ListBuffer()).toSeq
      }

      val results = resultInfos.map(res =>
          prepareResults(res.id, 0.0, res.time.toMillis, res.source, res.results)
      )

      Future.successful(CompoundQueryResponseInfoMessage(
        results
      ))

    } catch {
      case e: Exception => {
        log.error(e)
        Future.failed(e)
      }
    }
  }

  override def doBooleanQuery(request: SimpleBooleanQueryMessage): Future[QueryResponseInfoMessage] = {
    log.debug("rpc call for Boolean query operation")

    try {
      val bq = SearchRPCMethods.toQueryHolder(request)

      val results = QueryOp.booleanQuery(bq)

      Future.successful(prepareResults(request.queryid, 0.0, 0, "metadata", results))
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
  override def getCachedResults(request: CachedResultsMessage): Future[QueryResponseInfoMessage] = {
    log.debug("rpc call for cached query results")

    val cached = QueryHandler.getFromQueryCache(Option(request.queryid))

    if(cached.isSuccess){
      Future.successful(prepareResults(request.queryid, 0.0, 0, "cache", cached.get))
    } else {
      Future.failed(QueryNotCachedException())
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
  private def prepareResults(queryid : String, confidence : Double, time : Long, source : String, df : DataFrame): QueryResponseInfoMessage = {
    import org.apache.spark.sql.functions.{array, col, lit, udf}

    val asMap = udf((keys: Seq[String], values: Seq[String]) =>
      keys.zip(values).filter {
        case (k, null) => false
        case _ => true
      }.toMap)

    val cols = df.dtypes.slice(2, df.dtypes.length).map(_._1)

    val keys = array(cols.map(lit): _*)
    val values = array(cols.map(col): _*)

    val results = if (!cols.isEmpty) {
      df.withColumn("metadata", asMap(keys, values))
        .collect().map(row => QueryResultMessage(
        row.getAs[Long](FieldNames.idColumnName),
        row.getAs[Float](FieldNames.distanceColumnName),
        row.getMap[String, String](3).toMap
      ))
    } else {
      df
        .collect().map(row => QueryResultMessage(
        row.getAs[Long](FieldNames.idColumnName),
        row.getAs[Float](FieldNames.distanceColumnName),
        Map[String, String]()
      ))
    }

    QueryResponseInfoMessage(queryid, confidence, time, source, results)
  }
}
